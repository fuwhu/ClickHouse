#include "Databases/DatabaseFactory.h"
#include "Storages/Iceberg/IcebergCommon.h"
#include <filesystem>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Databases/DatabaseIceberg.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/Iceberg/StorageIceberg.h>

#include <Poco/Util/AbstractConfiguration.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    const extern int UNKNOWN_TABLE;
    const extern int NO_ELEMENTS_IN_CONFIG;
    const extern int BAD_ARGUMENTS;
}

static ASTPtr getColumnDeclaration(const DataTypePtr & data_type)
{
    WhichDataType which(data_type);

    if (which.isNullable())
        return makeASTFunction("Nullable", getColumnDeclaration(typeid_cast<const DataTypeNullable *>(data_type.get())->getNestedType()));

    if (which.isArray())
        return makeASTFunction("Array", getColumnDeclaration(typeid_cast<const DataTypeArray *>(data_type.get())->getNestedType()));

    if (which.isMap())
    {
        const auto * map_type = typeid_cast<const DataTypeMap *>(data_type.get());
        return makeASTFunction("Map", getColumnDeclaration(map_type->getKeyType()), getColumnDeclaration(map_type->getValueType()));
    }
    if (which.isTuple())
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(data_type.get());
        auto function = std::make_shared<ASTFunction>();

        function->name = "Tuple";
        function->arguments = std::make_shared<ASTExpressionList>();
        function->children.push_back(function->arguments);

        for (const auto & child_type : tuple_type->getElements())
        {
            function->arguments->children.push_back(getColumnDeclaration(child_type));
        }

        return function;
    }

    return std::make_shared<ASTIdentifier>(data_type->getName());
}

DatabaseIceberg::DatabaseIceberg(
    const String & database_name_, const String & metadata_path_, const IcebergCatalogConfig & config_, ContextPtr context_)
    : IDatabase(database_name_)
    , WithContext(context_->getGlobalContext())
    , metadata_path(metadata_path_)
    , config(config_)
    , log(&Poco::Logger::get("DatabaseIceberg(" + database_name_ + ")"))
{
    tryInit();
}

void DatabaseIceberg::tryInit() const
{
    try
    {
        /// check if iceberg database exists
        listIcebergTables(config.iceberg_api_server_uri, config.iceberg_database);
    }
    catch (...)
    {
        tryLogCurrentException(log);
    }
}

String DatabaseIceberg::getMetadataPath() const
{
    return metadata_path;
}

void DatabaseIceberg::shutdown()
{
}

void DatabaseIceberg::drop(ContextPtr /*context*/)
{
    fs::remove_all(getMetadataPath());
}

ASTPtr DatabaseIceberg::getCreateDatabaseQuery() const
{
    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());

    auto storage = std::make_shared<ASTStorage>();
    if (config.cluster.empty())
    {
        storage->set(storage->engine, makeASTFunction(getEngineName(), std::make_shared<ASTLiteral>(config.iceberg_database)));
    }
    else
    {
        storage->set(
            storage->engine,
            makeASTFunction(
                getEngineName(),
                std::make_shared<ASTLiteral>(config.iceberg_database),
                std::make_shared<ASTLiteral>(config.cluster),
                std::make_shared<ASTLiteral>(Poco::toLower(String(magic_enum::enum_name(config.distribution_mode))))));
    }

    create_query->set(create_query->storage, storage);

    return create_query;
}

bool DatabaseIceberg::isTableExist(const String & name, ContextPtr local_context) const
{
    return bool(tryGetTable(name, local_context));
}

StoragePtr DatabaseIceberg::tryGetTable(const String & name, ContextPtr local_context) const
{
    /// Lazy init
    StorageID table_id(getDatabaseName(), name);

    // return StorageIceberg::create(table_id, config, local_context);
    return std::make_shared<StorageIceberg>(table_id, config, local_context);
}

bool DatabaseIceberg::empty() const
{
    IcebergTables tables = listIcebergTables(config.iceberg_api_server_uri, config.iceberg_database);

    return tables.empty();
}

DatabaseTablesIteratorPtr
DatabaseIceberg::getTablesIterator(ContextPtr local_context, const FilterByNameFunction & filter_by_table_name, bool /*skip_not_loaded*/) const
{
    Tables tables;
    IcebergTables table_names = listIcebergTables(config.iceberg_api_server_uri, config.iceberg_database);

    if (!filter_by_table_name)
    {
        for (const auto & table_name : table_names)
        {
            auto table = tryGetTable(table_name, local_context);
            if (!table)
                continue;
            tables.emplace(table_name, table);
        }
    }
    else
    {
        for (const auto & table_name : table_names)
        {
            if (filter_by_table_name(table_name))
            {
                auto table = tryGetTable(table_name, local_context);
                if (!table)
                    continue;
                tables.emplace(table_name, table);
            }
        }
    }

    return std::make_unique<DatabaseTablesSnapshotIterator>(tables, getDatabaseName());
}

ASTPtr DatabaseIceberg::getCreateTableQueryImpl(const String & name, ContextPtr local_context, bool throw_on_error) const
{
    auto storage = tryGetTable(name, local_context);
    const auto * iceberg_table = storage->as<StorageIceberg>();
    try
    {
        iceberg_table->loadTable();
    }
    catch (...)
    {
        if (throw_on_error)
            throw;

        tryLogCurrentException(log);
        return {};
    }

    const auto & create_query = std::make_shared<ASTCreateQuery>();
    create_query->setDatabase(getDatabaseName());
    create_query->setTable(iceberg_table->getStorageID().getTableName());

    auto columns_declare_list = std::make_shared<ASTColumns>();
    auto columns_expression_list = std::make_shared<ASTExpressionList>();

    columns_declare_list->set(columns_declare_list->columns, columns_expression_list);
    create_query->set(create_query->columns_list, columns_declare_list);

    auto metadata_snapshot = iceberg_table->getInMemoryMetadataPtr();
    for (const auto & column_type_and_name : metadata_snapshot->getColumns().getOrdinary())
    {
        const auto & column_declaration = std::make_shared<ASTColumnDeclaration>();
        column_declaration->name = column_type_and_name.name;
        column_declaration->type = getColumnDeclaration(column_type_and_name.type);
        columns_expression_list->children.emplace_back(column_declaration);
    }

    auto storage_ast = std::make_shared<ASTStorage>();
    storage_ast->set(
        storage_ast->engine,
        makeASTFunction(
            "Iceberg",
            std::make_shared<ASTLiteral>(iceberg_table->getIcebergMetadata().database),
            std::make_shared<ASTLiteral>(iceberg_table->getIcebergMetadata().table),
            std::make_shared<ASTLiteral>(iceberg_table->getIcebergMetadata().current_snapshot_id)));

    storage_ast->set(storage_ast->partition_by, metadata_snapshot->getPartitionKeyAST());
    storage_ast->set(storage_ast->order_by, metadata_snapshot->getSortingKeyAST());
    create_query->set(create_query->storage, storage_ast);

    return create_query;
}

void registerDatabaseIceberg(DatabaseFactory & factory)
{
    auto create_fn = [](const DatabaseFactory::Arguments & args)
    {
        IcebergCatalogConfig config;
        const auto & global_conf = args.context->getConfigRef();
        if (!global_conf.has("iceberg_api_server_uri"))
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Can not create Iceberg catalog because iceberg_api_server_uri not found in config file");
        const auto & iceberg_api_server_uri = global_conf.getString("iceberg_api_server_uri");
        config.iceberg_api_server_uri = iceberg_api_server_uri;
        
        ASTs & engine_args = args.engine_args;
        if (engine_args.empty() || engine_args.size() > 3)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Bad definition of Iceberg catalog, should be Iceberg(iceberg_database_name, [cluster], [distribution_mode])"
                );
        
        config.iceberg_database = safeGetLiteralValue<String>(engine_args[0], "Iceberg");

        if (config.iceberg_database.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad definition of Iceberg catalog, iceberg database name can not be empty" );

        if (engine_args.size() >= 2)
        {
            config.cluster = safeGetLiteralValue<String>(engine_args[1], "Iceberg");
            /// check if cluster exists
            args.context->getCluster(config.cluster);
        }

        if (engine_args.size() == 3)
        {
            auto mode_str = safeGetLiteralValue<String>(engine_args[2], "Iceberg");
            auto mode_opt = magic_enum::enum_cast<DistributionMode>(Poco::toUpper(mode_str));
            if (!mode_opt.has_value())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Unknown distribution mode '{}' for Iceberg catalog, must be 'random' or 'consistent_hash'",
                    mode_str);

            config.distribution_mode = mode_opt.value();
        }

        return std::make_shared<DatabaseIceberg>(
            args.database_name,
            args.metadata_path,
            config,
            args.context
        );
    };

    factory.registerDatabase("Iceberg", create_fn, {.supports_arguments = true});
}
}
