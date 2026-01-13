#include <cstddef>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_CENTRALIZATION_DISABLED;
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
}

namespace
{


class FunctionSyncMetadata : public IFunction
{
public:
    static constexpr auto name = "syncMetadata";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionSyncMetadata>(context);
    }

    explicit FunctionSyncMetadata(ContextPtr context_) : context(context_) {}

    bool isVariadic() const override { return true; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isSuitableForConstantFolding() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool useDefaultImplementationForSparseColumns() const override { return false; }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        auto manager = context->getMetadataCentralizationManager();

        if (!manager)
        {
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_DISABLED,
                "Metadata centralization is not enabled while function syncMetadata is called.");
        }

        if (!manager->getBossAvailableFlag())
        {
            throw Exception(
                ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
                "Boss service is not available. syncMetadata operation is disabled. Last error: {}",
                manager->getBossLastError());
        }

        // Parse drop_sync parameter from arguments (default: false)
        bool drop_sync = false;
        if (!arguments.empty())
        {
            const auto * drop_sync_col = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
            if (drop_sync_col)
                drop_sync = drop_sync_col->getUInt(0) != 0;
        }

        manager->syncMetadataFromBoss(drop_sync);

        /// convertToFullColumnIfConst is needed for proper execution in distributed queries
        return DataTypeUInt8().createColumnConst(input_rows_count, 1u)->convertToFullColumnIfConst();
    }

    /// Override executeImplDryRun to prevent execution during the planning phase on the initiator node
    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// During dry run (planning phase), just return a dummy result without executing syncMetadata
        return DataTypeUInt8().createColumnConst(input_rows_count, 1u)->convertToFullColumnIfConst();
    }

private:
    ContextPtr context;
};

}

REGISTER_FUNCTION(SyncMetadata)
{
    factory.registerFunction<FunctionSyncMetadata>(FunctionDocumentation{
        .description = R"(
Triggers metadata synchronization from Boss storage.
This function acquires the local mutex and calls syncMetadataFromBoss.
It is designed to be called remotely from other cluster nodes during DDL operations.

Syntax: syncMetadata([drop_sync])
- drop_sync (optional): If set to 1, performs synchronous metadata update for drop table. Default is 0 (async).

Returns 1 on success.
)",
        .examples{
            {"syncMetadata", "SELECT syncMetadata()", "1"},
            {"syncMetadata_drop_sync", "SELECT syncMetadata(1)", "1"}
        },
        .category = FunctionDocumentation::Category::Other
    });
}

}
