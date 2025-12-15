#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Storages/Iceberg/StorageIceberg.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionIceberg.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <memory>
#include "registerTableFunctions.h"
#include <Parsers/ASTLiteral.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void TableFunctionIceberg::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.size() != 7)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, 
            "The signature of table function {} shall be the following:\n"
            " - database, table, snapshot_id, structure, sorting keys, order_id, partition keys",
            getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    /// This arguments are always the first
    String database = args[0]->as<ASTLiteral &>().value.safeGet<String>();
    String table = args[1]->as<ASTLiteral &>().value.safeGet<String>();
    Int64 snapshot_id = args[2]->as<ASTLiteral &>().value.safeGet<Int64>();
    String structure = args[3]->as<ASTLiteral &>().value.safeGet<String>();
    Array sorting_keys = args[4]->as<ASTLiteral &>().value.safeGet<Array>();

    std::vector<IcebergTableMetadata::SortingKey> keys;
    keys.reserve(sorting_keys.size());
    std::for_each(sorting_keys.begin(), sorting_keys.end(), [&](Field & field)
    {
        auto tuple = field.safeGet<Tuple>();
        String sort_name = tuple[0].safeGet<String>();
        int direction = tuple[1].safeGet<Int32>();
        int nulls_direction = tuple[2].safeGet<Int32>();
        IcebergTableMetadata::SortingKey key{sort_name, direction, nulls_direction};
        keys.emplace_back(std::move(key));
    });

    Int64 order_id = args[5]->as<ASTLiteral &>().value.safeGet<Int64>();
    Array partition_keys = args[6]->as<ASTLiteral &>().value.safeGet<Array>();

    std::vector<IcebergTableMetadata::PartitionKey> partitions;
    partitions.reserve(partition_keys.size());
    std::for_each(partition_keys.begin(), partition_keys.end(), [&](Field & field)
    {
        String partition = field.safeGet<String>();
        IcebergTableMetadata::PartitionKey key{partition};
        partitions.emplace_back(std::move(key));
    });

    auto columns = parseColumnsListFromString(structure, context);

    table_metadata.database = database;
    table_metadata.table = table;
    table_metadata.current_snapshot_id = snapshot_id;
    table_metadata.schema = columns.getOrdinary();
    table_metadata.sorting_keys = std::move(keys);
    table_metadata.order_id = order_id;
    table_metadata.partition_keys = std::move(partitions);
}

ColumnsDescription TableFunctionIceberg::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    ColumnsDescription description;
    for (const auto & col : table_metadata.schema)
    {
        description.add(ColumnDescription(col.name, col.type));
    }

    return description;
}

StoragePtr TableFunctionIceberg::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool /*is_insert_query*/) const
{
    StorageID table_id(getDatabaseName(), table_name);

    IcebergCatalogConfig config;
    const auto & global_conf = context->getConfigRef();
    const auto & iceberg_api_server_uri = global_conf.getString("iceberg_api_server_uri");
    config.iceberg_api_server_uri = iceberg_api_server_uri;

    // return StorageIceberg::create(table_id, config, table_metadata, context);
    return std::make_shared<StorageIceberg>(table_id, config, table_metadata, context);
}

void registerTableFunctionIceberg(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionIceberg>();
}
}
