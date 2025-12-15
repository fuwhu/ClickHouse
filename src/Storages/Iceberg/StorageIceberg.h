#pragma once

#include "Interpreters/ActionsDAG.h"
#include <optional>
#include <Storages/IStorage.h>
#include <Storages/Iceberg/IcebergCommon.h>
#include <QueryPipeline/RemoteQueryExecutor.h>
#include "Common/Macros.h"

namespace DB
{
class StorageIceberg final : public IStorage
{
public:
    enum FilesSortingStatus
    {
        SORTED = 0,
        NOT_SORTED = 1,
        UNKNOWN = 2
    };

    StorageIceberg(StorageID table_id, const IcebergCatalogConfig & iceberg_config_, ContextPtr context_);

    StorageIceberg(
        StorageID table_id,
        const IcebergCatalogConfig & iceberg_config_,
        const IcebergTableMetadata & iceberg_metadata_,
        ContextPtr context_);

    String getName() const override { return "Iceberg"; }

    std::optional<UInt64> totalRows(ContextPtr) const override;

    std::optional<UInt64> totalBytes(ContextPtr) const override;

    std::optional<UInt64> totalRowsByPartitionPredicate(const ActionsDAG &, ContextPtr) const override;

    Pipe read(
        const Names & /*column_names*/,
        const StorageSnapshotPtr & /*storage_snapshot*/,
        SelectQueryInfo & /*query_info*/,
        ContextPtr /*context*/,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t /*max_block_size*/,
        size_t /*num_streams*/) override;

    QueryProcessingStage::Enum
    getQueryProcessingStage(ContextPtr, QueryProcessingStage::Enum, const StorageSnapshotPtr &, SelectQueryInfo &) const override;

    void loadTable() const;

    const IcebergTableMetadata & getIcebergMetadata() const;

    StorageMetadataPtr getInMemoryMetadataPtr() const override;

    bool canMoveConditionsToPrewhere() const override { return true; }

    bool supportsPrewhere() const override { return true; }

    ColumnSizeByName getColumnSizes() const override;

    bool supportsTrivialCountOptimization(const StorageSnapshotPtr & /*storage_snapshot*/, ContextPtr /*query_context*/) const override { return true; }

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
        FilesSortingStatus files_sorting_status = FilesSortingStatus::UNKNOWN);

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
        FilesSortingStatus files_sorting_status,
        String query,
        ConnectionPoolWithFailoverPtr connection_pool,
        Block header,
        QueryProcessingStage::Enum processed_stage,
        std::ostringstream& filter_expression_oss);
    
    void checkPartitionKeyInFilter(SelectQueryInfo & query_info) const;

    IcebergCatalogConfig iceberg_config;
    mutable IcebergTableMetadata iceberg_metadata;
    ContextPtr context;

    mutable bool init;
    mutable std::mutex mutex;

    LoggerPtr log;
    FileCachePtr cache;
};
}
