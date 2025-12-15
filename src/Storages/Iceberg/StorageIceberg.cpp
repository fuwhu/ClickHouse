#include "StorageIceberg.h"
#include <memory>
#include <optional>
#include "DataTypes/DataTypesNumber.h"
#include "IO/WriteBufferFromString.h"
#include "IcebergFileSource.h"
#include "IcebergKeyCondition.h"
#include "Interpreters/ActionsDAG.h"
#include "Interpreters/ExpressionActionsSettings.h"
#include "Interpreters/ExpressionActions.h"
#include "Interpreters/getHeaderForProcessingStage.h"
#include "Storages/ColumnsDescription.h"
#include "Storages/Iceberg/IcebergCommon.h"

#include <consistent_hashing.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeArray.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/ClusterProxy/SelectStreamFactory.h>
#include <Interpreters/ClusterProxy/executeQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/Sources/RemoteSource.h>
#include <Storages/SelectQueryInfo.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Parser.h>
#include <Poco/Logger.h>
#include "Common/Logger.h"
#include "Common/logger_useful.h"

namespace ProfileEvents
{
extern const Event IcebergAnalysisElapsedMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric ORCCreateThreads;
    extern const Metric ORCCreageThreadsActive;
    extern const Metric ORCCreateThreadsScheduled;
}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool separate_sorted_and_non_sorted_iceberg_file_processing;
}

namespace
{
    ASTPtr rewriteSelectQuery(const ASTPtr & query, const IcebergTableMetadata & iceberg_metadata)
    {
        auto modified_query_ast = query->clone();

        ASTSelectQuery & select_query = modified_query_ast->as<ASTSelectQuery &>();

        String schema;
        for (const auto & name_and_type : iceberg_metadata.schema)
        {
            if (!schema.empty())
                schema.append(", ");
            schema.append("`" + name_and_type.name + "`"); /// If the column has "-", it would have syntax error
            schema.append(" ");
            schema.append(name_and_type.type->getName());
        }

        Array sort_arr = {};
        std::for_each(iceberg_metadata.sorting_keys.begin(), iceberg_metadata.sorting_keys.end(), 
        [&](const IcebergTableMetadata::SortingKey & key)
        {
            Tuple tuple{key.name, key.direction, key.nulls_direction};
            sort_arr.push_back(tuple);
        });

        Array partition_arr = {};
        std::for_each(iceberg_metadata.partition_keys.begin(), iceberg_metadata.partition_keys.end(),
        [&](const IcebergTableMetadata::PartitionKey & key)
        {
            partition_arr.push_back(key.name);
        });
        
        ASTPtr table_func_ptr = makeASTFunction(
            "icebergFiles",
            std::make_shared<ASTLiteral>(iceberg_metadata.database), /// database
            std::make_shared<ASTLiteral>(iceberg_metadata.table), /// table
            std::make_shared<ASTLiteral>(iceberg_metadata.current_snapshot_id), /// snapshot id
            std::make_shared<ASTLiteral>(schema), /// schema
            std::make_shared<ASTLiteral>(sort_arr), /// sorting key for RIO
            std::make_shared<ASTLiteral>(iceberg_metadata.order_id), /// order_id
            std::make_shared<ASTLiteral>(partition_arr) /// partition keys
        );

        select_query.addTableFunction(table_func_ptr);

        return modified_query_ast;
    }

    Block pack_stats(std::vector<IcebergDataFile> & data_files, const IcebergTableMetadata & iceberg_metadata)
    {   
        const auto & schema = iceberg_metadata.schema;
        const auto & sorting_keys = iceberg_metadata.sorting_keys;
        Block block;
        size_t idx = 0;
        std::for_each(sorting_keys.begin(), sorting_keys.end(), [&](const IcebergTableMetadata::SortingKey & sorting_key)
        {
            const String & name = sorting_key.name;
            std::optional<NameAndTypePair> name_and_type = schema.tryGetByName(name);
            if (!name_and_type.has_value())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Sorting key {} isn't in schema, it's a bug", name);
            auto type = name_and_type->type;
            auto min_column = type->createColumn();
            auto max_column = type->createColumn();

            for (auto & data_file : data_files)
            {
                if (!data_file.sorting_key_id.has_value() || data_file.sorting_key_id.value() != iceberg_metadata.order_id)
                {
                    min_column->insertDefault();
                    max_column->insertDefault();
                }
                else 
                {
                    if (idx < data_file.statistics.min.values.size())
                        min_column->insert(data_file.statistics.min.values[idx]);
                    else
                        data_file.sorting_key_id = std::nullopt;

                    if (idx < data_file.statistics.max.values.size())
                        max_column->insert(data_file.statistics.max.values[idx]);
                    else
                        data_file.sorting_key_id = std::nullopt;
                }
            }
            
            block.insert({std::move(min_column), type, "min_" + name});
            block.insert({std::move(max_column), type, "max_" + name});
            idx++;
        });

        return block;
    }

    Field unpack_field(TypeIndex type, ColumnPtr col, size_t idx)
    {
        switch (type) 
        {
            case TypeIndex::Int32:
                return (*col)[idx].safeGet<Int32>();
            case TypeIndex::Int64:
                return (*col)[idx].safeGet<Int64>();
            case TypeIndex::UInt8:
                return (*col)[idx].safeGet<UInt8>();
            case TypeIndex::String:
                return (*col)[idx].safeGet<String>();
            case TypeIndex::Float32:
                return (*col)[idx].safeGet<Float32>();
            case TypeIndex::Float64:
                return (*col)[idx].safeGet<Float64>();
            case TypeIndex::Date:
                return (*col)[idx].safeGet<UInt16>();
            case TypeIndex::DateTime64:
                return (*col)[idx].safeGet<Decimal64>();
            case TypeIndex::UUID:
                return (*col)[idx].safeGet<UUID>();
            case TypeIndex::Decimal32:
                return (*col)[idx].safeGet<Decimal32>();
            case TypeIndex::Decimal64:
                return (*col)[idx].safeGet<Decimal64>();
            case TypeIndex::Decimal128:
                return (*col)[idx].safeGet<Decimal128>();
            case TypeIndex::Decimal256:
                return (*col)[idx].safeGet<Decimal256>();
            default:
                return Field{};
        }
    }

    void unpack_stats(const Block & block, const IcebergTableMetadata & iceberg_metadata, std::vector<IcebergDataFile> & data_files)
    {   
        if (block.rows() != data_files.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "block row size should be same with number of data files");
        
        const auto & schema = iceberg_metadata.schema;

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col_with_type_name = block.getByPosition(i);

            for (size_t j = 0; j < data_files.size(); ++j)
            {
                const auto & names = schema.getNames();
                auto type = col_with_type_name.type;
                /// for min value
                if (i % 2 == 0)
                {
                    auto it = std::find(names.begin(), names.end(), col_with_type_name.name.substr(4));
                    if (it == names.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "unpack statistics failed, it's a bug");
                    data_files[j].statistics.min.keys.push_back(it - names.begin() + 1);
                    data_files[j].statistics.min.values.push_back(DB::unpack_field(type->isNullable() ? typeid_cast<const DataTypeNullable *>(type.get())->getNestedType()->getTypeId() : type->getTypeId(), col_with_type_name.column, j));
                }
                /// for max value
                else 
                {
                    auto it = std::find(names.begin(), names.end(), col_with_type_name.name.substr(4));
                    if (it == names.end())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "unpack statistics failed, it's a bug");
                    data_files[j].statistics.max.keys.push_back(it - names.begin() + 1);
                    data_files[j].statistics.max.values.push_back(DB::unpack_field(type->isNullable() ? typeid_cast<const DataTypeNullable *>(type.get())->getNestedType()->getTypeId() : type->getTypeId(), col_with_type_name.column, j));   
                }
            }
            
        }
    }

    Block pack_files(const std::vector<IcebergDataFile> & data_files)
    {
        ColumnString::MutablePtr column_path = ColumnString::create();
        ColumnString::MutablePtr column_format = ColumnString::create();
        MutableColumns columns_index(3);
        columns_index[0] = ColumnInt64::create();
        columns_index[1] = ColumnString::create();
        columns_index[2] = ColumnInt64::create();
        ColumnArray::MutablePtr column_indicies = ColumnArray::create(ColumnTuple::create(std::move(columns_index)));
        ColumnInt64::MutablePtr column_size = ColumnInt64::create();
        ColumnInt64::MutablePtr column_sort_order_id = ColumnInt64::create();

        std::for_each(
            data_files.begin(),
            data_files.end(),
            [&](const IcebergDataFile & file)
            {
                column_path->insertData(file.path.data(), file.path.size());
                column_format->insertData(file.format.data(), file.format.size());

                auto arr = Array{};
                for (const auto & index : file.indicies)
                    arr.push_back(Tuple{index.index_id, index.index_data, index.correlated_table_snapshot});
                column_indicies->insert(std::move(arr));
                /// size
                column_size->insertValue(file.size);
                /// sort-order-id
                if (file.sorting_key_id.has_value())
                    column_sort_order_id->insertValue(file.sorting_key_id.value());
                else
                    column_sort_order_id->insertValue(-1); /// unpack should treat -1 as null   
            });

        Block block{
            {std::move(column_path), std::make_shared<DataTypeString>(), "path"},
            {std::move(column_format), std::make_shared<DataTypeString>(), "format"},
            {std::move(column_indicies),
             std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(
                 DataTypes{std::make_shared<DataTypeInt64>(), std::make_shared<DataTypeString>(), std::make_shared<DataTypeInt64>()})),
             "indicies"},
            {std::move(column_size), std::make_shared<DataTypeInt64>(), "size"},
            {std::move(column_sort_order_id), std::make_shared<DataTypeInt64>(), "sort_order_id"}};

        return block;
    }

    std::vector<IcebergDataFile> unpack_files(const Block & block)
    {
        std::vector<IcebergDataFile> data_files;
        data_files.resize(block.rows());

        auto column_path = block.getByName("path").column;
        auto column_format = block.getByName("format").column;
        auto column_indicies = block.getByName("indicies").column;
        auto column_size = block.getByName("size").column;
        auto column_sort_order_id = block.getByName("sort_order_id").column;

        for (size_t i = 0; i < column_path->size(); ++i)
        {
            data_files[i].path = (*column_path)[i].safeGet<String>();
            data_files[i].format = (*column_format)[i].safeGet<String>();
            data_files[i].size = (*column_size)[i].safeGet<Int64>();
            Int64 sort_key_id = (*column_sort_order_id)[i].safeGet<Int64>();
            if (sort_key_id == -1)
                data_files[i].sorting_key_id = std::nullopt;
            else
                data_files[i].sorting_key_id = sort_key_id;

            auto arr = (*column_indicies)[i].safeGet<Array>();
            for (const auto & f : arr)
            {
                auto tuple = f.safeGet<Tuple>();
                data_files[i].indicies.push_back(
                    {.index_id = tuple[0].safeGet<Int64>(),
                     .index_data = tuple[1].safeGet<String>(),
                     .correlated_table_snapshot = tuple[2].safeGet<Int64>()});
            }
        }

        return data_files;
    }

    void setStorageMetadata(StorageInMemoryMetadata & storage_metadata, const IcebergTableMetadata & iceberg_metadata, ContextPtr context)
    {
        ColumnsDescription description;
        for (const auto & col : iceberg_metadata.schema)
        {
            description.add(ColumnDescription(col.name, col.type));
        }

        /// partition key
        auto partition_keys = iceberg_metadata.partition_keys;

        if (!partition_keys.empty())
        {
            String key_string;
            for (size_t i = 0, size = partition_keys.size(); i < size; ++i)
            {
                key_string += i == 0 ? "" : ", ";
                key_string += partition_keys[i].name;
            }
            auto partition_key = KeyDescription::parse(key_string, description, context, false);
            storage_metadata.partition_key = partition_key;
        }

        /// sorting key
        auto sorting_keys = iceberg_metadata.sorting_keys;
        if (!sorting_keys.empty())
        {
            String key_string;
            for (size_t i = 0, size = sorting_keys.size(); i < size; ++i)
            {
                key_string += i == 0 ? "" : ", ";
                key_string += sorting_keys[i].name;
            }
            auto sorting_key = KeyDescription::parse(key_string, description, context, true);
            storage_metadata.sorting_key = sorting_key;
        }

        storage_metadata.setColumns(std::move(description));
    }
}

StorageIceberg::StorageIceberg(StorageID table_id, const IcebergCatalogConfig & iceberg_config_, ContextPtr context_)
    : IStorage(table_id)
    , iceberg_config(iceberg_config_)
    , context(context_)
    , init(false)
    , log(getLogger(table_id.getNameForLogs()))
{
    cache = getCachePtrForDisk("iceberg", context->getConfigRef(), "iceberg", context);
}

StorageIceberg::StorageIceberg(
    StorageID table_id, const IcebergCatalogConfig & iceberg_config_, const IcebergTableMetadata & iceberg_metadata_, ContextPtr context_)
    : IStorage(table_id)
    , iceberg_config(iceberg_config_)
    , iceberg_metadata(iceberg_metadata_)
    , context(context_)
    , init(true)
    , log(getLogger(table_id.getNameForLogs()))
{
    StorageInMemoryMetadata storage_metadata;
    setStorageMetadata(storage_metadata, iceberg_metadata, context);
    setInMemoryMetadata(storage_metadata);
    cache = getCachePtrForDisk("iceberg", context->getConfigRef(), "iceberg", context);
}

std::optional<UInt64> StorageIceberg::totalRows(ContextPtr) const
{
    return iceberg_metadata.total_records;
}

std::optional<UInt64> StorageIceberg::totalBytes(ContextPtr) const
{
    return iceberg_metadata.total_files_size;
}

std::optional<UInt64> StorageIceberg::totalRowsByPartitionPredicate(const ActionsDAG & filter_actions_dag, ContextPtr local_context) const
{
    if (iceberg_metadata.total_records.has_value() && iceberg_metadata.total_records.value() == 0)
        return 0;

    std::vector<String> partition_column_names;
    std::for_each(
        iceberg_metadata.partition_keys.begin(),
        iceberg_metadata.partition_keys.end(),
        [&](const auto & p) { partition_column_names.push_back(p.name); });

    // ExpressionActionsSettings actions_settings(local_context);
    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag.getOutputs().front(), local_context);
    ActionsDAG dag(iceberg_metadata.schema);
    
    IcebergKeyCondition condition(
        inverted_dag,
        local_context,
        partition_column_names,
        std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(local_context)));

    auto filter_expr = IcebergExpression(condition.getExpressionTree(true));
    if (filter_expr.emptyOrTrue())
        return {};

    auto scan_result = scanIcebergTable(
        iceberg_config.iceberg_api_server_uri,
        iceberg_metadata.database,
        iceberg_metadata.table,
        iceberg_metadata.current_snapshot_id,
        partition_column_names,
        filter_expr);

    if (scan_result.files.empty())
        return 0;

    if (scan_result.residual_expression.emptyOrTrue())
    {
        UInt64 total_rows = 0;
        std::for_each(scan_result.files.begin(), scan_result.files.end(), [&](const auto & file) { total_rows += file.statistics.count; });
        return total_rows;
    }

    return {};
}

void StorageIceberg::loadTable() const
{
    if (init)
        return;

    std::lock_guard lock(mutex);
    if (init)
        return;

    iceberg_metadata
        = loadIcebergTable(iceberg_config.iceberg_api_server_uri, iceberg_config.iceberg_database, getStorageID().getTableName());

    StorageInMemoryMetadata storage_metadata;
    setStorageMetadata(storage_metadata, iceberg_metadata, context);
    metadata.set(std::make_unique<StorageInMemoryMetadata>(storage_metadata));

    init = true;
}

const IcebergTableMetadata & StorageIceberg::getIcebergMetadata() const
{
    loadTable();
    return iceberg_metadata;
}

StorageMetadataPtr StorageIceberg::getInMemoryMetadataPtr() const
{
    loadTable();
    return metadata.get();
}

void StorageIceberg::checkPartitionKeyInFilter(SelectQueryInfo & query_info) const
{
    auto & query = query_info.query->as<ASTSelectQuery &>();
    const auto & metadata = getIcebergMetadata();        
    auto partition_keys = metadata.partition_keys;
    
    if (partition_keys.empty()) 
        return;

    const auto & where = query.where();
    const auto & prewhere = query.prewhere();
    
    if (!where && !prewhere)
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
            "Filter required on {}.{} for at least one partition column: {}", 
            metadata.database, 
            metadata.table,
            metadata.partition_keys[0].name);

    const String & where_name = where ? where->getColumnName() : "";
    const String & prewhere_name = prewhere ? prewhere->getColumnName() : "";
    
    bool has_partition_condition = false;
    std::for_each(partition_keys.begin(), partition_keys.end(), [&](IcebergTableMetadata::PartitionKey & partition_key)
    {
        if (where_name.find(partition_key.name) != std::string::npos 
                || prewhere_name.find(partition_key.name) != std::string::npos)
        {
            has_partition_condition = true;
            return;
        }
    });
    if (!has_partition_condition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, 
                "Filter required on {}.{} for at least one partition column: {}", 
                metadata.database, 
                metadata.table,
                metadata.partition_keys[0].name);
}

Pipe StorageIceberg::read(
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    size_t max_block_size,
    size_t num_streams)
{
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::IcebergAnalysisElapsedMicroseconds, watch.elapsedMicroseconds()); });

    checkPartitionKeyInFilter(query_info);

    Names format_columns_names;
    bool need_file_column = false;
    for (const auto & column : column_names)
    {
        if (column == "_file")
            need_file_column = true;
        else
            format_columns_names.emplace_back(column);
    }

    Block source_block = getHeaderForProcessingStage(column_names, storage_snapshot, query_info, local_context, processed_stage);

    if (local_context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        if (iceberg_metadata.total_records.has_value() && iceberg_metadata.total_records.value() == 0)
            return Pipe(std::make_shared<NullSource>(std::move(source_block)));

        ActionsDAG dag(iceberg_metadata.schema);

        IcebergKeyCondition condition(
            query_info,
            local_context,
            column_names,
            std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(local_context)));

        auto filter_expr = IcebergExpression(condition.getExpressionTree(true));
        
        /// For now, we just want the sorting key stats, maybe someday need other function to set this string array.
        /// And stats in scan_result.data_file also should distinguish sorting keys and other keys.
        std::vector<String> stats_keys;
        stats_keys.reserve(iceberg_metadata.sorting_keys.size());
        for (auto & key : iceberg_metadata.sorting_keys)
        {
            stats_keys.emplace_back(key.name);
        }

        auto scan_result = scanIcebergTable(
            iceberg_config.iceberg_api_server_uri,
            iceberg_metadata.database,
            iceberg_metadata.table,
            iceberg_metadata.current_snapshot_id,
            stats_keys,
            filter_expr);

        if (scan_result.files.empty())
            return Pipe(std::make_shared<NullSource>(std::move(source_block)));

        if (iceberg_config.cluster.empty())
            return readFromLocal(
                storage_snapshot,
                query_info,
                local_context,
                processed_stage,
                std::move(source_block),
                std::move(scan_result.files),
                scan_result.residual_expression,
                max_block_size,
                num_streams,
                format_columns_names,
                need_file_column);
        else
            return readFromRemote(
                storage_snapshot,
                query_info,
                local_context,
                processed_stage,
                std::move(source_block),
                std::move(scan_result.files),
                scan_result.residual_expression,
                max_block_size,
                num_streams,
                format_columns_names,
                need_file_column);
    }
    else
    {
        auto scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};
        if (!scalars.contains("_iceberg_files"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not get needed iceberg files from context scalars");
        if (!scalars.contains("_iceberg_filters"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not get needed iceberg filters from context scalars");
        if (!scalars.contains("_iceberg_files_sorting_status"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not get needed iceberg files sorting status from context scalars");

        std::vector<IcebergDataFile> data_files = unpack_files(scalars["_iceberg_files"]);

        IcebergExpression filter_expr;
        String filter_str = (*(scalars["_iceberg_filters"].getByName("iceberg_filters").column))[0].safeGet<String>();
        filter_expr.deserialize(filter_str);
        FilesSortingStatus files_sorting_status = static_cast<FilesSortingStatus>((*(scalars["_iceberg_files_sorting_status"].getByName("iceberg_files_sorting_status").column))[0].safeGet<UInt8>());

        return readFromLocal(
            storage_snapshot,
            query_info,
            local_context,
            processed_stage,
            std::move(source_block),
            std::move(data_files),
            filter_expr,
            max_block_size,
            num_streams,
            format_columns_names,
            need_file_column,
            files_sorting_status);
    }
}

Pipe StorageIceberg::readFromLocal(
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum /*processed_stage*/,
    Block source_block,
    std::vector<IcebergDataFile> data_files,
    IcebergExpression & filter_expression,
    size_t max_block_size,
    unsigned num_streams,
    const Names & format_columns_names,
    bool need_file_column,
    FilesSortingStatus files_sorting_status)
{
    /// Check if all files have the same sorting key with the table sorting key, if not, then disable the RIO.
    if (files_sorting_status == FilesSortingStatus::UNKNOWN)
    {
        Int64 table_sorting_key_id = iceberg_metadata.order_id;
        for (const auto & file : data_files)
        {
            if (!file.sorting_key_id.has_value() || table_sorting_key_id != file.sorting_key_id.value())
            {
                LOG_DEBUG(log, "the file {} has sorting key id {}, but metadata has key id {}",
                    file.path, file.sorting_key_id.has_value() ? file.sorting_key_id.value() : -1, table_sorting_key_id);
                query_info.input_order_info = nullptr;
                break;
            }
        }
    }
    else if (files_sorting_status == FilesSortingStatus::NOT_SORTED)
    {
        LOG_DEBUG(log, "The files from initial query node are not sorted, so disable the read-in-order optimzation.");
        query_info.input_order_info = nullptr;
    }

    /// We only unpack the statistics in case the RIO is applicable for non-initial query.
    if (local_context->getClientInfo().query_kind != ClientInfo::QueryKind::INITIAL_QUERY
        && query_info.input_order_info)
    {
        auto scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};
        // Note : the scalar '_iceberg_files_stats' must have already be set by initial node here.
        if (!scalars.contains("_iceberg_files_stats"))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not get the iceberg files statitics from context scalars while the read-in-order is enabled.");
        unpack_stats(scalars["_iceberg_files_stats"], iceberg_metadata, data_files);
    }

    auto format_columns_description
        = ColumnsDescription{storage_snapshot->getSampleBlockForColumns(format_columns_names).getNamesAndTypesList()};

    auto col_desc = std::make_shared<ColumnsDescription>(format_columns_description);
    auto column_names = format_columns_description.getNamesOfPhysical();
    
    auto filter_actions_dag = query_info.filter_actions_dag;
    ActionsDAGWithInversionPushDown inverted_dag(filter_actions_dag ? filter_actions_dag->getOutputs().front() : nullptr, local_context);
    ActionsDAG dag(iceberg_metadata.schema);

    auto key_condition = std::make_shared<KeyCondition>(
        inverted_dag,
        local_context,
        column_names,
        std::make_shared<ExpressionActions>(std::move(dag), ExpressionActionsSettings(local_context)));

    // std::vector<IcebergFilePtr> iceberg_files;
    auto iceberg_files = std::make_unique<std::vector<IcebergFilePtr>>();
    iceberg_files->reserve(data_files.size());
    std::mutex mu;
    IcebergTableMetaWithUri table_meta = {
        iceberg_config.iceberg_api_server_uri,
        iceberg_metadata.database,
        iceberg_metadata.table,
        iceberg_metadata.current_snapshot_id
    };

    Block sample_block_for_all ;
    auto tmp_block = storage_snapshot->getSampleBlockForColumns(format_columns_description.getNamesOfPhysical());
    if (query_info.prewhere_info)
    {
        sample_block_for_all = query_info.prewhere_info->prewhere_actions.updateHeader(tmp_block);
    }
    else 
        sample_block_for_all = tmp_block;

    auto create_files = [&, sample_block_for_all, col_desc]
    (const std::shared_ptr<std::vector<size_t>> & indices, bool spread_splits)
    {
        auto metrics_index = local_context->newIcebergCreateFileMetrics();
        Stopwatch stop_watch;
        auto current_time_us = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
        local_context->setCreateFileStartTime(metrics_index, current_time_us);  
        UInt64 total_apply_filters_time_cost_us = 0;
        for (auto index : *indices)
        {
            auto & data_file = data_files[index];
            IcebergFileScanResult scan_result;
            scan_result.data_file = data_file;

            UInt64 apply_filters_time_cost_us;
            auto iceberg_file = IcebergFileSource::createIcebergFile(
                std::move(scan_result),
                local_context,
                query_info,
                storage_snapshot,
                *col_desc,
                sample_block_for_all,
                max_block_size,
                key_condition,
                apply_filters_time_cost_us,
                spread_splits,
                cache);
            total_apply_filters_time_cost_us += apply_filters_time_cost_us;

            if (iceberg_file)
            {
                std::lock_guard lock(mu);
                iceberg_files->push_back(std::move(iceberg_file));
            }
        }
        local_context->setCreateIcebergFileTimeCostMicroseconds(metrics_index, stop_watch.elapsedMicroseconds());
        if (spread_splits)
            local_context->setApplyFiltersTimeCostMicroseconds(metrics_index, total_apply_filters_time_cost_us);
    };

    LOG_DEBUG(log, "Before creating iceberg files, there are {} iceberg files to create.", data_files.size());
    auto spread_splits = data_files.size() < num_streams;

    if (num_streams <= 1)
    {
        auto file_indices = std::make_shared<std::vector<size_t>>(data_files.size());
        std::iota(file_indices->begin(), file_indices->end(), 0);
        create_files(file_indices, spread_splits);
    }
    else
    {
        ThreadPool pool(CurrentMetrics::ORCCreateThreads, CurrentMetrics::ORCCreageThreadsActive, CurrentMetrics::ORCCreateThreadsScheduled, std::min(static_cast<size_t>(num_streams), data_files.size()));
        UInt32 batch_size = data_files.size() < num_streams ? 1 : data_files.size()/num_streams;
        auto indices_to_create = std::make_shared<std::vector<size_t>>();
        UInt32 threads_scheduled = 0;
        for (size_t i = 0; i < data_files.size(); ++i)
        {
            indices_to_create->emplace_back(i);
            if (indices_to_create->size() == batch_size)
            {
                pool.scheduleOrThrowOnError(
                    [&, indices_to_create, thread_group = CurrentThread::getGroup()]
                    {
                        if (thread_group)
                            CurrentThread::attachToGroupIfDetached(thread_group);
                        create_files(indices_to_create, spread_splits);
                    });
                ++threads_scheduled;
                indices_to_create = std::make_shared<std::vector<size_t>>();
            }
        }
        if (!indices_to_create->empty())
        {
            pool.scheduleOrThrowOnError(
                [&, indices_to_create, thread_group = CurrentThread::getGroup()]
                {
                    if (thread_group)
                        CurrentThread::attachToGroupIfDetached(thread_group);
                    create_files(indices_to_create, spread_splits);
                });
            ++threads_scheduled;
        }
        LOG_DEBUG(log, "Totally {} threads scheduled to create iceberg files.", threads_scheduled);
        pool.wait();
    }

    auto read_type = ReadType::Default;
    if (query_info.input_order_info && query_info.input_order_info->sort_description_for_merging.size() == 1)
    {
        auto input_order_info = query_info.input_order_info;
        read_type = iceberg_metadata.sorting_keys.front().direction == input_order_info->direction
            ? ReadType::InOrder
            : ReadType::InReverseOrder;
        auto res = IcebergFileSource::spreadFilesOrSplitsAmongStreamsWithOrder(
                                    std::move(iceberg_files), local_context, input_order_info, read_type,
                                    iceberg_metadata.sorting_keys.front().direction, std::move(source_block), format_columns_description,
                                    need_file_column, num_streams, filter_expression, table_meta, key_condition, spread_splits);
        return res;
    }
    Pipe res;
    if (spread_splits)
        res = IcebergFileSource::spreadSplitsAmongStreams(
                std::move(iceberg_files), local_context, std::move(source_block),
                format_columns_description, need_file_column, num_streams,
                filter_expression, table_meta, key_condition);
    else
        res = IcebergFileSource::spreadFilesAmongStreams(
                std::move(iceberg_files), local_context, std::move(source_block),
                format_columns_description, need_file_column, num_streams,
                filter_expression, table_meta, key_condition);
    return res;
}

std::shared_ptr<RemoteQueryExecutor> StorageIceberg::constructRemoteQueryExecutor(
        ContextPtr local_context,
        std::vector<IcebergDataFile>& files,
        FilesSortingStatus files_sorting_status,
        String query,
        ConnectionPoolWithFailoverPtr connection_pool,
        Block header,
        QueryProcessingStage::Enum processed_stage,
        std::ostringstream& filter_expression_oss)
{
    auto scalars = local_context->hasQueryContext() ? local_context->getQueryContext()->getScalars() : Scalars{};
    scalars["_iceberg_filters"] = Block{{DataTypeString().createColumnConst(1, filter_expression_oss.str()), std::make_shared<DataTypeString>(), "iceberg_filters"}};
    scalars["_iceberg_files"] = pack_files(files);
    scalars["_iceberg_files_sorting_status"] = Block{{DataTypeUInt8().createColumnConst(1, static_cast<UInt8>(files_sorting_status)), std::make_shared<DataTypeUInt8>(), "iceberg_files_sorting_status"}};
    // Send the scalar '_iceberg_files_stats' to remote node only when the table has sorting key and the files aren't known to be non-sorted.
    if (!iceberg_metadata.sorting_keys.empty() && files_sorting_status != FilesSortingStatus::NOT_SORTED)
        scalars["_iceberg_files_stats"] = pack_stats(files, iceberg_metadata);

    auto remote_query_executor = std::make_shared<RemoteQueryExecutor>(
        connection_pool,
        query,
        header,
        local_context,
        /*throttler=*/nullptr,
        scalars,
        Tables(),
        processed_stage);
    return remote_query_executor;
}

Pipe StorageIceberg::readFromRemote(
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr local_context,
    QueryProcessingStage::Enum processed_stage,
    Block source_block,
    std::vector<IcebergDataFile> data_files,
    IcebergExpression & filter_expression,
    size_t max_block_size,
    unsigned num_streams,
    const Names & format_columns_names,
    bool need_file_column)
{
    auto cluster = local_context->getCluster(iceberg_config.cluster);
    if (cluster->getShardsInfo().empty())
        return readFromLocal(
            storage_snapshot,
            query_info,
            local_context,
            processed_stage,
            std::move(source_block),
            std::move(data_files),
            filter_expression,
            max_block_size,
            num_streams,
            format_columns_names,
            need_file_column);

    query_info.cluster = cluster;
    auto new_query = rewriteSelectQuery(query_info.query, iceberg_metadata);
    std::ostringstream oss;
    oss.exceptions(std::ios::failbit);
    Poco::JSON::Stringifier::stringify(filter_expression.serialize(), oss);
    bool add_agg_info = processed_stage == QueryProcessingStage::WithMergeableState;
    std::unordered_map<size_t, std::vector<IcebergDataFile>> files_per_shard;
    auto hasher = std::hash<String>();
    for (auto & file : data_files)
    {
        size_t idx;
        if (iceberg_config.distribution_mode == DistributionMode::CONSISTENT_HASH)
            idx = ConsistentHashing(hasher(file.path), cluster->getShardsInfo().size());
        else
            idx = thread_local_rng() % cluster->getShardsInfo().size();

        files_per_shard[idx].emplace_back(std::move(file));
    }

    Pipes pipes;
    Int64 table_sorting_key_id = iceberg_metadata.order_id;
    for (size_t i = 0, size = cluster->getShardsInfo().size(); i < size; ++i)
    {
        auto &files = files_per_shard[i];

        WriteBufferFromOwnString buf;
        IAST::FormatSettings settings(true);
        IAST::FormatState state;
        IAST::FormatStateStacked state_stacked;
        IAST::FormattingBuffer format_buf{buf, settings, state, state_stacked};
        new_query->format(format_buf);

        // Do not separate the sorted and non-sorted files in the cases below:
        // 1. the input_order_info is null which means the RIO is applicable.
        // 2. the setting `separate_sorted_and_non_sorted_iceberg_file_processing` is false.
        if (query_info.input_order_info == nullptr || !local_context->getSettingsRef()[Setting::separate_sorted_and_non_sorted_iceberg_file_processing])
        {
            auto remote_query_executor = constructRemoteQueryExecutor(
                local_context,
                files,
                FilesSortingStatus::UNKNOWN,
                buf.str(),
                cluster->getShardsInfo()[i].pool,
                source_block,
                processed_stage,
                oss);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false, false));
            continue;
        }

        std::vector<IcebergDataFile> sorted_files;
        std::vector<IcebergDataFile> non_sorted_files;
        for (auto& file : files)
        {
            if (file.sorting_key_id.has_value() && table_sorting_key_id == file.sorting_key_id.value())
                sorted_files.emplace_back(std::move(file));
            else
                non_sorted_files.emplace_back(std::move(file));
        }
        if (!sorted_files.empty())
        {            
            auto remote_query_executor = constructRemoteQueryExecutor(
                local_context,
                sorted_files,
                FilesSortingStatus::SORTED,
                buf.str(),
                cluster->getShardsInfo()[i].pool,
                source_block,
                processed_stage,
                oss);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false, false));
        }
        if (!non_sorted_files.empty())
        {
            auto remote_query_executor = constructRemoteQueryExecutor(
                local_context,
                non_sorted_files,
                FilesSortingStatus::NOT_SORTED,
                buf.str(),
                cluster->getShardsInfo()[i].pool,
                source_block,
                processed_stage,
                oss);
            pipes.emplace_back(std::make_shared<RemoteSource>(remote_query_executor, add_agg_info, false, false));
        }
    }

    return Pipe::unitePipes(std::move(pipes));
}

QueryProcessingStage::Enum StorageIceberg::getQueryProcessingStage(
    ContextPtr local_context, QueryProcessingStage::Enum to_stage, const StorageSnapshotPtr &, SelectQueryInfo &) const
{
    if (local_context->getClientInfo().query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        return QueryProcessingStage::Enum::FetchColumns;

    if (iceberg_config.cluster.empty())
        return QueryProcessingStage::Enum::FetchColumns;

    auto cluster = local_context->getCluster(iceberg_config.cluster);
    auto shard_count = cluster->getLocalShardCount() + cluster->getRemoteShardCount();

    if (shard_count == 0)
        return QueryProcessingStage::FetchColumns;

    if (to_stage >= QueryProcessingStage::Enum::WithMergeableState)
        return QueryProcessingStage::Enum::WithMergeableState;

    return QueryProcessingStage::Enum::FetchColumns;
}

using ColumnSizeByName = std::unordered_map<std::string, ColumnSize>;
ColumnSizeByName StorageIceberg::getColumnSizes() const {
    ColumnSizeByName column_sizes;
    auto in_memory_metadata = getInMemoryMetadataPtr();
    if (!in_memory_metadata)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "In-Memory metadata of storage iceberg is null while getting column sizes.");
    for (const auto & column : in_memory_metadata->getColumns())
        column_sizes[column.name] = ColumnSize{0};

    return column_sizes;
}
}
