#pragma once

#include <optional>
#include <Storages/IStorage.h>
#include <Storages/Iceberg/IcebergCommon.h>
#include <base/shared_ptr_helper.h>
#include <QueryPipeline/RemoteQueryExecutor.h>

namespace DB
{
class StorageIceberg final : public shared_ptr_helper<StorageIceberg>, public IStorage
{
public:
    StorageIceberg(StorageID table_id, const IcebergCatalogConfig & iceberg_config_, ContextPtr context_);

    StorageIceberg(
        StorageID table_id,
        const IcebergCatalogConfig & iceberg_config_,
        const IcebergTableMetadata & iceberg_metadata_,
        ContextPtr context_);

    String getName() const override { return "Iceberg"; }

    std::optional<UInt64> totalRows(const Settings &) const override;

    std::optional<UInt64> totalBytes(const Settings &) const override;

    std::optional<UInt64> totalRowsByPartitionPredicate(const SelectQueryInfo &, ContextPtr) const override;

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    bool supportsIndexForIn() const override { return true; }

    bool mayBenefitFromIndexForIn(const ASTPtr &, ContextPtr, const StorageMetadataPtr &) const override { return true; }

    void loadTable() const;

    const IcebergTableMetadata & getIcebergMetadata() const;

    StorageMetadataPtr getInMemoryMetadataPtr() const override;

    NamesAndTypesList getVirtuals() const override;

    bool canMoveConditionsToPrewhere() const override { return true; }

    bool supportsPrewhere() const override { return true; }

    ColumnSizeByName getColumnSizes() const override;

private:
    Pipe readFromLocal(
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        Block source_block,
        std::vector<IcebergDataFile> data_files,
        IcebergExpression & filter_expression,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/,
        const Names & format_columns_names,
        bool need_file_column,
        std::optional<bool> files_sorted = std::nullopt);

    Pipe readFromRemote(
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        Block source_block,
        std::vector<IcebergDataFile> data_files,
        IcebergExpression & filter_expression,
        size_t /*max_block_size*/,
        unsigned /*num_streams*/,
        const Names & format_columns_names,
        bool need_file_column);

    std::shared_ptr<RemoteQueryExecutor> constructRemoteQueryExecutor(
        ContextPtr local_context,
        std::vector<IcebergDataFile>& files,
        std::optional<bool> files_sorted,
        String query,
        ConnectionPoolWithFailoverPtr connection_pool,
        Block header,
        QueryProcessingStage::Enum processed_stage,
        std::ostringstream& filter_expression_oss);

    IcebergCatalogConfig iceberg_config;
    mutable IcebergTableMetadata iceberg_metadata;
    ContextPtr context;

    mutable bool init;
    mutable std::mutex mutex;

    Poco::Logger * log;
    FileCachePtr cache;
};
}
