#pragma once

#include <Storages/Iceberg/IcebergCommon.h>
#include <TableFunctions/ITableFunction.h>

namespace DB
{
class TableFunctionIceberg final : public ITableFunction
{
public:
    static constexpr auto name = "icebergFiles";

    std::string getName() const override { return name; }

    ColumnsDescription getActualTableStructure(ContextPtr /*context*/) const override;

protected:
    StoragePtr executeImpl(
        const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;

    const char * getStorageTypeName() const override { return "IcebergFiles"; }

    void parseArguments(const ASTPtr &, ContextPtr) override;

private:
    IcebergTableMetadata table_metadata;
};

}
