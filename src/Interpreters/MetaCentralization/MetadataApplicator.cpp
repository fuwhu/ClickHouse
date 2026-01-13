#include <Interpreters/MetaCentralization/MetadataApplicator.h>

#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Core/Settings.h>
#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseOnDisk.h>
#include <Disks/IStoragePolicy.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <Interpreters/MetaCentralization/TableDetector.h>
#include <IO/Boss/BossClient.h>
#include <IO/ReadHelpers.h>
#include <IO/SharedThreadPools.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Storages/MergeTree/ReplicatedMergeTreeTableMetadata.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
    extern const int NO_ZOOKEEPER;
}

namespace Setting
{
    extern const SettingsBool fsync_metadata;
}

MetadataApplicator::MetadataApplicator(MetadataCentralizationManager * manager_, ContextMutablePtr context_)
    : WithMutableContext(context_)
    , manager(manager_)
    , table_detector(std::make_unique<TableDetector>(getContext()))
    , log(getLogger("MetadataApplicator"))
{
}

MetadataApplicator::~MetadataApplicator() = default;

void MetadataApplicator::applyDatabaseChanges(const Database & db, const String & db_sql)
{
    auto & catalog = DatabaseCatalog::instance();
    ASTPtr ast = parseDatabaseSQL(db_sql, db.name);
    ASTCreateQuery * create_query = ast->as<ASTCreateQuery>();

    if (catalog.isDatabaseExist(db.name))
    {
        LOG_DEBUG(log, "Database {} already exists, updating", backQuoteIfNeed(db.name));
        updateDatabase(db);
    }
    else
    {
        LOG_INFO(log, "Creating new database: {}", backQuoteIfNeed(db.name));
        create_query->attach = false;
        createDatabase(db, *create_query, db_sql);
    }
}

void MetadataApplicator::applyTableChanges(const Table & table, const String & database_name, const String & table_sql)
{
    UUID table_uuid = parseUUIDFromString(table.uuid);
    auto & catalog = DatabaseCatalog::instance();

    StorageID storage_id(database_name, table.name, table_uuid);

    auto table_ptr = catalog.tryGetTable(storage_id, getContext());

    ASTPtr ast = parseTableSQL(table_sql, database_name, table.name);
    ASTCreateQuery * create_query = ast->as<ASTCreateQuery>();

    if (table_ptr)
    {
        LOG_DEBUG(log, "Table {}.{} already exists, updating", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
        updateTable(table, database_name, table_uuid, *create_query);
    }
    else
    {
        LOG_DEBUG(log, "Table {}.{} not found in catalog, determining scenario", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
        auto scenario = detectTableScenario(*create_query, catalog);

        if (scenario.should_attach)
        {
            LOG_INFO(log, "Scenario 2 detected for table {}.{}: data_exists={}, replica_in_zk={}, is_replicated={}",
                     backQuoteIfNeed(database_name), backQuoteIfNeed(table.name), scenario.data_exists, scenario.replica_in_zk, scenario.is_replicated);
            LOG_INFO(log, "Metadata was lost but data/replica exists, attaching table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));

            createAndRegisterTable(table, ast, LoadingStrictnessLevel::ATTACH);
            LOG_INFO(log, "Table {}.{} attached successfully (metadata recovered)", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
        }
        else
        {
            LOG_INFO(log, "Scenario 1 detected for table {}.{}: creating new table", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
            create_query->attach = false;
            createAndRegisterTable(table, ast, LoadingStrictnessLevel::CREATE);
            LOG_INFO(log, "Table {}.{} created successfully", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
        }
    }
}

void MetadataApplicator::createDatabase(const Database & db, const ASTCreateQuery & create_query, const String & db_sql) const
{
    LOG_INFO(log, "Creating database from Boss: {}", backQuoteIfNeed(db.name));

    String database_name = db.name;
    auto db_disk = getContext()->getDatabaseDisk();

    fs::path metadata_path = fs::path("store") / DatabaseCatalog::getPathForUUID(parseUUIDFromString(db.uuid));

    DatabasePtr database = DatabaseFactory::instance().get(create_query, metadata_path / "", getContext());

    writeDatabaseMetadataFile(db_disk, database_name, db_sql);

    try
    {
        DatabaseCatalog::instance().attachDatabase(database_name, database);
        LOG_INFO(log, "Database {} created successfully", backQuoteIfNeed(db.name));
    }
    catch (...)
    {
        DatabaseCatalog::instance().detachDatabase(getContext(), database_name, false, false);
        throw;
    }
}

/// TODO: Implement rename database
void MetadataApplicator::updateDatabase(const Database & db) const
{
    LOG_DEBUG(log, "Updating database: {}", backQuoteIfNeed(db.name));
}

/// TODO: Implement rename table
void MetadataApplicator::updateTable(const Table & table, const String & database_name, const UUID & table_uuid, ASTCreateQuery & create_query) const
{
    LOG_INFO(log, "Updating existing table: {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));

    auto table_id = StorageID(database_name, table.name, table_uuid);
    StoragePtr table_ptr = DatabaseCatalog::instance().tryGetTable(table_id, getContext());

    if (!table_ptr)
    {
        LOG_ERROR(log, "Table {}.{} not found locally, will create instead", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table {}.{} not found locally", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
    }

    auto new_metadata = InterpreterCreateQuery::getMetadataFromCreateQuery(create_query, getContext());

    if (dynamic_cast<StorageReplicatedMergeTree *>(table_ptr.get()))
        updateZooKeeperMetadataForReplicatedTable(table_ptr, new_metadata);

    table_ptr->setInMemoryMetadata(new_metadata);

    DatabaseCatalog::instance().getDatabase(database_name)->alterTable(getContext(), table_id, new_metadata);

    LOG_INFO(log, "Table {}.{} updated successfully", backQuoteIfNeed(database_name), backQuoteIfNeed(table.name));
}

void MetadataApplicator::dropTable(const String & database_name, const String & table_name, const String & table_uuid, bool drop_sync) const
{
    try
    {
        LOG_INFO(log, "Dropping table {}.{} with UUID {} (drop_sync={})",
                 backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), table_uuid, drop_sync);

        dropTableImpl(database_name, table_name, drop_sync);

        if (drop_sync)
            DatabaseCatalog::instance().waitTableFinallyDropped(parseUUIDFromString(table_uuid));

        LOG_INFO(log, "Successfully dropped table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to drop table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        throw;
    }
}

void MetadataApplicator::dropDatabase(const String & database_name, const String & database_uuid, bool drop_sync) const
{
    try
    {
        LOG_INFO(log, "Dropping database {} with UUID {} (drop_sync={})", backQuoteIfNeed(database_name), database_uuid, drop_sync);

        std::vector<std::pair<String, UUID>> tables;
        dropDatabaseImpl(database_name, drop_sync, tables);

        if (drop_sync)
        {
            for (const auto & [_, table_uuid] : tables)
                DatabaseCatalog::instance().waitTableFinallyDropped(table_uuid);
        }

        LOG_INFO(log, "Successfully dropped database {}", backQuoteIfNeed(database_name));
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to drop database {}", backQuoteIfNeed(database_name)));
        throw;
    }
}

void MetadataApplicator::updateZooKeeperMetadataForReplicatedTable(
    const StoragePtr & storage,
    const StorageInMemoryMetadata & new_metadata) const
{
    LOG_INFO(log, "Updating ZooKeeper metadata for replicated table: {}", storage->getStorageID().getNameForLogs());

    auto * replicated_storage = dynamic_cast<StorageReplicatedMergeTree *>(storage.get());
    
    LOG_INFO(log, "Overwriting ZooKeeper metadata for replicated table (no log entries)");

    try
    {
        // Get the specific ZooKeeper instance that this table is configured to use
        // This is important because ClickHouse supports multiple ZooKeeper clusters
        const String & zookeeper_name = replicated_storage->getZooKeeperName();
        auto zookeeper = getContext()->getDefaultOrAuxiliaryZooKeeper(zookeeper_name);
        if (!zookeeper)
        {
            throw Exception(ErrorCodes::NO_ZOOKEEPER,
                "Cannot get ZooKeeper connection for cluster '{}'", zookeeper_name);
        }

        const String & zookeeper_path = replicated_storage->getZooKeeperPath();
        const String & replica_path = replicated_storage->getReplicaPath();

        // Get current metadata snapshot with new metadata applied
        auto metadata_snapshot = std::make_shared<StorageInMemoryMetadata>(new_metadata);

        // Generate new metadata strings
        ReplicatedMergeTreeTableMetadata table_metadata(*replicated_storage, metadata_snapshot);
        String new_metadata_str = table_metadata.toString();
        String new_columns_str = new_metadata.getColumns().toString();

        LOG_DEBUG(log, "Using ZooKeeper cluster '{}' for table metadata overwrite", zookeeper_name);

        // Batch all set operations together
        Coordination::Requests ops;
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/metadata", new_metadata_str, -1));
        ops.emplace_back(zkutil::makeSetRequest(zookeeper_path + "/columns", new_columns_str, -1));
        ops.emplace_back(zkutil::makeSetRequest(replica_path + "/metadata", new_metadata_str, -1));
        ops.emplace_back(zkutil::makeSetRequest(replica_path + "/columns", new_columns_str, -1));

        // Execute all operations in a single batch
        zookeeper->multi(ops);

        LOG_DEBUG(log, "Overwrote common metadata in ZooKeeper at {}/metadata", zookeeper_path);
        LOG_DEBUG(log, "Overwrote common columns in ZooKeeper at {}/columns", zookeeper_path);
        LOG_DEBUG(log, "Overwrote replica metadata in ZooKeeper at {}/metadata", replica_path);
        LOG_DEBUG(log, "Overwrote replica columns in ZooKeeper at {}/columns", replica_path);

        LOG_INFO(log, "Successfully overwrote ZooKeeper metadata for replicated table");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to overwrite ZooKeeper metadata for replicated table");
        throw;
    }

    LOG_INFO(log, "Successfully updated ZooKeeper metadata for replicated table: {}",
             storage->getStorageID().getNameForLogs());
}

ASTPtr MetadataApplicator::parseDatabaseSQL(const String & db_sql, const String & database_name) const
{
    try
    {
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, db_sql, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        auto * create_query = ast->as<ASTCreateQuery>();

        if (!create_query || !create_query->storage || !create_query->storage->engine || create_query->storage->engine->name != "Atomic")
        {
            log->error(
                "Invalid database SQL from Boss for database {}: missing storage or engine, or engine is not Atomic",
                backQuoteIfNeed(database_name));
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid database SQL from Boss for database {}: missing storage or engine, or engine is not Atomic",
                backQuoteIfNeed(database_name));
        }

        create_query->setDatabase(database_name);
        return ast;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to parse database SQL from Boss for database {}", backQuoteIfNeed(database_name)));
        throw;
    }
}

ASTPtr MetadataApplicator::parseTableSQL(const String & table_sql, const String & database_name, const String & table_name) const
{
    try
    {
        ParserCreateQuery parser;
        ASTPtr ast = parseQuery(parser, table_sql, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
        auto * create_query = ast->as<ASTCreateQuery>();

        if (!create_query || !create_query->storage || !create_query->storage->engine)
        {
            tryLogCurrentException(
                log,
                fmt::format(
                    "Invalid table SQL from Boss for database {} table {}: missing storage or engine",
                    backQuoteIfNeed(database_name),
                    backQuoteIfNeed(table_name)));
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Invalid table SQL from Boss for database {} table {}: missing storage or engine",
                backQuoteIfNeed(database_name),
                backQuoteIfNeed(table_name));
        }

        create_query->setDatabase(database_name);
        create_query->setTable(table_name);

        return ast;
    }
    catch (...)
    {
        tryLogCurrentException(
            log,
            fmt::format(
                "Failed to parse table SQL from Boss for table {}.{} due to syntax or parsing errors",
                backQuoteIfNeed(database_name),
                backQuoteIfNeed(table_name)));
        throw;
    }
}

std::pair<DatabasePtr, String> MetadataApplicator::getDatabaseAndDataPath(const ASTCreateQuery & create_query) const
{
    DatabasePtr database = DatabaseCatalog::instance().getDatabase(create_query.getDatabase());
    String data_path = database->getTableDataPath(create_query);
    return {database, data_path};
}

StoragePtr MetadataApplicator::createStorage(
    ASTCreateQuery & create_query,
    const String & data_path,
    LoadingStrictnessLevel mode) const
{
    auto metadata_from_query = InterpreterCreateQuery::getMetadataFromCreateQuery(create_query, getContext());
    return StorageFactory::instance().get(
        create_query,
        data_path,
        getContext(),
        getContext()->getGlobalContext(),
        metadata_from_query.columns,
        metadata_from_query.constraints,
        mode,
        false);
}

UUID MetadataApplicator::parseUUIDFromString(const String & uuid_str)
{
    return parseUUID(std::span<const UInt8>(
        reinterpret_cast<const UInt8*>(uuid_str.data()),
        uuid_str.size()
    ));
}

MetadataApplicator::TableScenario MetadataApplicator::detectTableScenario(const ASTCreateQuery & create_query, const DatabaseCatalog & catalog) const
{
    String expected_data_path;
    String database_name = create_query.getDatabase();
    String table_name = create_query.getTable();
    try
    {
        DatabasePtr database = catalog.getDatabase(database_name);
        expected_data_path = database->getTableDataPath(create_query);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to determine expected data path for table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        throw;
    }

    TableScenario scenario{};

    scenario.is_replicated = create_query.storage->engine->name.starts_with("Replicated");

    if (!expected_data_path.empty())
        scenario.data_exists = table_detector->isTableDataPathExisting(create_query, expected_data_path);

    if (scenario.is_replicated)
        scenario.replica_in_zk = table_detector->isReplicaInZooKeeper(create_query);

    scenario.should_attach = scenario.data_exists || scenario.replica_in_zk;

    return scenario;
}

void MetadataApplicator::writeDatabaseMetadataFile(
    DiskPtr disk,
    const String & database_name,
    const String & sql_content) const
{
    String database_name_escaped = escapeForFileName(database_name);
    fs::path metadata_dir_path("metadata");
    disk->createDirectories(metadata_dir_path);

    fs::path metadata_file_tmp_path = metadata_dir_path / (database_name_escaped + ".sql.tmp");
    fs::path metadata_file_path = metadata_dir_path / (database_name_escaped + ".sql");

    disk->removeFileIfExists(metadata_file_tmp_path);

    writeMetadataFile(
        disk,
        /*file_path=*/metadata_file_tmp_path,
        /*content=*/sql_content,
        /*fsync_metadata=*/getContext()->getSettingsRef()[Setting::fsync_metadata]);

    try
    {
        disk->moveFile(metadata_file_tmp_path, metadata_file_path);
    }
    catch (...)
    {
        disk->removeFileIfExists(metadata_file_tmp_path);
        if (disk->existsFile(metadata_file_path))
            disk->removeFileIfExists(metadata_file_path);
        throw;
    }
}

void MetadataApplicator::createAndRegisterTable(
    const Table & table,
    ASTPtr ast,
    LoadingStrictnessLevel mode) const
{
    auto * create_query = ast->as<ASTCreateQuery>();
    auto [database, data_path] = getDatabaseAndDataPath(*create_query);

    StoragePtr storage = createStorage(*create_query, data_path, mode);

    database->createTable(getContext(), table.name, storage, ast);
    storage->startup();
}

void MetadataApplicator::dropTableImpl(const String & database_name, const String & table_name, bool drop_sync) const 
{
    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    auto table = database->getTable(table_name, getContext());

    table->flushAndShutdown(true);

    database->dropTable(getContext(), table_name, drop_sync);
}

void MetadataApplicator::dropDatabaseImpl(const String & database_name, bool drop_sync, std::vector<std::pair<String, UUID>> & tables) const
{
    auto database = DatabaseCatalog::instance().tryGetDatabase(database_name);

    // Collect all tables in the database
    auto table_context = Context::createCopy(getContext());
    table_context->setInternalQuery(true);

    for (auto it = database->getTablesIterator(table_context); it->isValid(); it->next())
    {
        auto table_ptr = it->table();
        StorageID storage_id = table_ptr->getStorageID();
        String table_name = storage_id.table_name;
        UUID table_uuid = database->tryGetTableUUID(table_name);
        tables.emplace_back(table_name, table_uuid);
        LOG_DEBUG(log, "Found table {}.{} to drop (UUID: {})", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), toString(table_uuid));
    }

    LOG_INFO(log, "Database {} has {} tables to drop", backQuoteIfNeed(database_name), tables.size());

    // Drop all tables in the database
    for (const auto & [table_name, table_uuid] : tables)
    {
        LOG_DEBUG(log, "Dropping table {}.{} (UUID: {})", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), toString(table_uuid));

        try
        {
            dropTableImpl(database_name, table_name, drop_sync);
        }
        catch (...)
        {
            tryLogCurrentException(log, fmt::format("Failed to drop table {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        }
    }

    if (drop_sync)
    {
        for (const auto & [table_name, table_uuid] : tables)
        {
            try
            {
                database->waitDetachedTableNotInUse(table_uuid);
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Failed to wait for table {}.{} to be dropped", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
            }
        }
    }

    DatabaseCatalog::instance().detachDatabase(getContext(), database_name, true, database->shouldBeEmptyOnDetach());
}

}
