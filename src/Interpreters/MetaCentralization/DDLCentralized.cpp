#include <Interpreters/MetaCentralization/DDLCentralized.h>

#include <Common/DateLUT.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/quoteString.h>
#include <IO/Boss/BossClient.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Databases/DatabasesCommon.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/MetaCentralization/ManifestCache.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTFunction.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Storages/AlterCommands.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

namespace CurrentMetrics
{
    extern const Metric AttachedTable;
    extern const Metric AttachedReplicatedTable;
}

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_database_num_to_throw;
    extern const ServerSettingsUInt64 max_replicated_table_num_to_throw;
    extern const ServerSettingsUInt64 max_table_num_to_throw;
}

namespace Setting
{
    extern const SettingsBool check_referential_table_dependencies;
    extern const SettingsBool check_table_dependencies;
    extern const SettingsBool database_atomic_wait_for_drop_and_detach_synchronously;
    extern const SettingsFloat ignore_drop_queries_probability;
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int DATABASE_ALREADY_EXISTS;
    extern const int TOO_MANY_DATABASES;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int METADATA_CENTRALIZATION_DISABLED;
    extern const int METADATA_CENTRALIZATION_DDL_DISABLED;
    extern const int METADATA_CENTRALIZATION_DIST_LOCK_TIMEOUT;
    extern const int METADATA_CENTRALIZATION_LOCAL_LOCK_TIMEOUT;
    extern const int METADATA_CENTRALIZATION_SYNC_FAILED;
    extern const int TOO_MANY_TABLES;
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
    extern const int ILLEGAL_COLUMN;
}

DDLCentralized::DDLCentralized(ContextPtr context_)
    : WithContext(context_)
    , log(getLogger("DDLCentralized"))
{
    manager = getContext()->getMetadataCentralizationManager();
}

DDLCentralized::~DDLCentralized() = default;

void DDLCentralized::checkBossAvailableFlag(const String & operation_description) const
{
    if (!manager->getBossAvailableFlag())
    {
        LOG_ERROR(log, "Boss service is not available, DDL operation '{}' is disabled", operation_description);
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "Boss service is not available. Metadata centralized DDL operations are disabled. "
            "Last error: {}",
            manager->getBossLastError());
    }
}

void DDLCentralized::syncBeforeOperation(const String & operation_description)
{
    LOG_DEBUG(log, "Syncing metadata from Boss before {}", operation_description);

    bool update_flag = manager->syncMetadataFromBoss();

    if (!update_flag)
    {
        LOG_DEBUG(log, "Not all updates were successful. The local and remote versions are not consistent.");
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_SYNC_FAILED,
            "Not all updates were successful. The local and remote versions are not consistent.");
    }
}

void DDLCentralized::validateDatabaseCreation(const String & database_name, bool if_not_exists)
{
    if (DatabaseCatalog::instance().isDatabaseExist(database_name))
    {
        if (if_not_exists)
            LOG_WARNING(log, "Database {} already exists, skipping creation (if_not_exists=true)", backQuoteIfNeed(database_name));

        throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Database {} already exists.", backQuoteIfNeed(database_name));
    }
}

void DDLCentralized::validateDatabaseLimit()
{
    auto db_num_limit = getContext()->getGlobalContext()->getServerSettings()[ServerSetting::max_database_num_to_throw].value;
    if (db_num_limit > 0)
    {
        size_t db_count = DatabaseCatalog::instance().getDatabases().size();
        std::initializer_list<std::string_view> system_databases = {
            DatabaseCatalog::TEMPORARY_DATABASE,
            DatabaseCatalog::SYSTEM_DATABASE,
            DatabaseCatalog::INFORMATION_SCHEMA,
            DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE,
        };

        for (const auto & system_database : system_databases)
        {
            if (db_count > 0 && DatabaseCatalog::instance().isDatabaseExist(std::string(system_database)))
                --db_count;
        }

        if (db_count >= db_num_limit)
        {
            throw Exception(
                ErrorCodes::TOO_MANY_DATABASES,
                "Too many databases. "
                "The limit (server configuration parameter `max_database_num_to_throw`) is set to {}, the current number of databases is {}",
                db_num_limit,
                db_count);
        }
    }
}

UUID DDLCentralized::prepareDatabase(ASTCreateQuery & create, const String & database_name)
{
    if (create.uuid == UUIDHelpers::Nil)
        create.uuid = UUIDHelpers::generateV4();

    fs::path metadata_path = fs::path("store") / DatabaseCatalog::getPathForUUID(create.uuid);
    auto db_disk = getContext()->getDatabaseDisk();

    if (!create.attach && db_disk->existsDirectory(metadata_path) && !db_disk->isDirectoryEmpty(metadata_path))
        throw Exception(ErrorCodes::DATABASE_ALREADY_EXISTS, "Metadata directory {} already exists and is not empty", metadata_path.string());

    LOG_DEBUG(log, "Generated UUID {} for database {}", toString(create.uuid), backQuoteIfNeed(database_name));

    create.setDatabase(TABLE_WITH_UUID_NAME_PLACEHOLDER);
    create.attach = true;
    create.if_not_exists = false;

    return create.uuid;
}

UUID DDLCentralized::prepareTable(ASTCreateQuery & create, const String & database_name, const String & table_name)
{
    create.uuid = UUIDHelpers::generateV4();
    LOG_DEBUG(log, "Generated UUID {} for table {}.{}", toString(create.uuid), backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    /// TODO "SETTINGS storage_policy = 'hot_and_cold'", Check whether the storage_policy exists.

    const String & engine_name = create.storage->engine->name;
    if (engine_name.ends_with("MergeTree")) 
    {
        bool replicated = engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree");
        const auto & initial_storage_settings = replicated ? getContext()->getReplicatedMergeTreeSettings() : getContext()->getMergeTreeSettings();
        std::unique_ptr<MergeTreeSettings> storage_settings = std::make_unique<MergeTreeSettings>(initial_storage_settings);
        storage_settings->loadFromQuery(*create.storage, getContext(), true);
    }

    create.database.reset();
    create.setTable(TABLE_WITH_UUID_NAME_PLACEHOLDER);
    create.attach = true;
    create.if_not_exists = false;

    return create.uuid;
}

String DDLCentralized::generateSQL(ASTCreateQuery & create)
{
    WriteBufferFromOwnString statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false, /*hilite=*/false);
    create.format(statement_buf, format_settings);
    writeChar('\n', statement_buf);
    String sql = statement_buf.str();
    if (!sql.empty() && sql.back() == '\n')
        sql.pop_back();
    return sql;
}

String DDLCentralized::applyAlterCommands(
    const String & database_name,
    const String & table_name,
    UUID table_uuid,
    AlterCommands & alter_commands)
{
    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    if (!database)
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Database {} does not exist", backQuoteIfNeed(database_name));

    auto table_id = StorageID(database_name, table_name, table_uuid);
    StoragePtr table = DatabaseCatalog::instance().tryGetTable(table_id, getContext());
    if (!table)
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Could not find table: {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    StorageInMemoryMetadata new_metadata = table->getInMemoryMetadata();
    alter_commands.validate(table, getContext());
    alter_commands.prepare(new_metadata);
    table->checkAlterIsPossible(alter_commands, getContext());
    alter_commands.apply(new_metadata, getContext());

    auto create_query = database->getCreateTableQuery(table_name, getContext());
    applyMetadataChangesToCreateQuery(create_query, new_metadata, getContext());

    auto & create = create_query->as<ASTCreateQuery &>();
    create.database.reset();
    create.setTable(TABLE_WITH_UUID_NAME_PLACEHOLDER);
    create.attach = true;

    WriteBufferFromOwnString statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false, /*hilite=*/false);
    create.format(statement_buf, format_settings);
    writeChar('\n', statement_buf);
    String new_table_sql = statement_buf.str();
    if (!new_table_sql.empty() && new_table_sql.back() == '\n')
        new_table_sql.pop_back();

    return new_table_sql;
}

String DDLCentralized::getDatabaseBossKey(const String & database, UInt32 version) const
{
    return fmt::format("{}_{}.sql", database, version);
}

String DDLCentralized::getTableBossKey(const String & database_uuid, const String & table, UInt32 version) const
{
    return fmt::format("{}/{}_{}.sql", database_uuid, table, version);
}

String DDLCentralized::getCurrentTimestamp() const
{
    time_t now = time(nullptr);
    return DateLUT::instance().timeToString(now);
}

std::reference_wrapper<Database> DDLCentralized::findDatabaseOrThrow(
    ManifestPtr & manifest,
    const UUID & database_uuid,
    const String & database_name)
{
    auto db_opt = manifest->findDatabase(toString(database_uuid));
    if (!db_opt)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Database {} (UUID: {}) not found in manifest",
            backQuoteIfNeed(database_name),
            toString(database_uuid));
    }
    return db_opt->get();
}

void DDLCentralized::executeCreateDatabase(ASTCreateQuery & create)
{
    String database_name = create.getDatabase();
    LOG_INFO(log, "Executing centralized CREATE DATABASE for {}", backQuoteIfNeed(database_name));
    String operation_description = fmt::format("CREATE DATABASE {}", backQuoteIfNeed(database_name));

    try
    {
        checkBossAvailableFlag(operation_description);   

        {
            auto lock = manager->acquireDistLock(operation_description);
            syncBeforeOperation(operation_description);

            validateDatabaseCreation(database_name, create.if_not_exists);
            validateDatabaseLimit();

            TemporaryLockForUUIDDirectory uuid_lock{prepareDatabase(create, database_name)};
            String database_sql = generateSQL(create);

            LOG_DEBUG(log, "Generated database SQL for {}: {}", backQuoteIfNeed(database_name), database_sql);

            submitDatabaseToBoss(database_name, create.uuid, database_sql);
        }

        LOG_DEBUG(log, "Syncing metadata from Boss after {}", operation_description);
        syncMetadataOfAllNodes(false);

        LOG_INFO(log, "CREATE DATABASE {} completed successfully", backQuoteIfNeed(database_name));
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::DATABASE_ALREADY_EXISTS && create.if_not_exists)
            return;

        tryLogCurrentException(log, fmt::format("Failed to execute CREATE DATABASE {}", backQuoteIfNeed(database_name)));
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to execute CREATE DATABASE {}", backQuoteIfNeed(database_name)));
        throw;
    }
}

void DDLCentralized::executeCreateTable(ASTCreateQuery & create)
{
    String database_name = create.getDatabase();
    String table_name = create.getTable();
    LOG_INFO(log, "Executing centralized CREATE TABLE for {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    String operation_description = fmt::format("CREATE TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    try
    {
        checkBossAvailableFlag(operation_description);

        {
            auto lock = manager->acquireDistLock(operation_description);
            syncBeforeOperation(operation_description);

            auto table_info = prepareTableForUpload(database_name, table_name, create);

            LOG_DEBUG(log, "Generated table SQL for {}.{}: {}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), table_info.table_sql);

            submitTableToBoss(database_name, table_name, table_info.database_uuid, table_info.table_uuid, table_info.table_sql);
        }

        LOG_DEBUG(log, "Syncing metadata from Boss after {}", operation_description);
        syncMetadataOfAllNodes(false);

        LOG_INFO(log, "CREATE TABLE {}.{} completed successfully", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::TABLE_ALREADY_EXISTS && create.if_not_exists)
            return;

        tryLogCurrentException(log, fmt::format("Failed to execute CREATE TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to execute CREATE TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        throw;
    }
}

void DDLCentralized::executeAlterTable(const String & database_name, const String & table_name, AlterCommands & alter_commands)
{
    LOG_INFO(log, "Executing centralized ALTER TABLE for {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    String operation_description = fmt::format("ALTER TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    try
    {
        checkBossAvailableFlag(operation_description);

        {
            auto lock = manager->acquireDistLock(operation_description);
            syncBeforeOperation(operation_description);

            auto database = DatabaseCatalog::instance().getDatabase(database_name);
            UUID database_uuid = database->getUUID();
            UUID table_uuid = database->tryGetTableUUID(table_name);

            String new_table_sql = applyAlterCommands(database_name, table_name, table_uuid, alter_commands);

            LOG_DEBUG(log, "Generated table SQL for {}.{}: {}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), new_table_sql);

            submitTableToBoss(database_name, table_name, database_uuid, table_uuid, new_table_sql);
        }

        LOG_DEBUG(log, "Syncing metadata from Boss after {}", operation_description);
        syncMetadataOfAllNodes(false);

        LOG_INFO(log, "ALTER TABLE {}.{} completed successfully", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to execute ALTER TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        throw;
    }
}

template <typename Func>
void DDLCentralized::modifyManifest(const String & operation_name, Func && modifier, ManifestPtr & manifest)
{
    try
    {
        modifier();

        manifest->last_modified = getCurrentTimestamp();
        manifest->version++;

        manager->getManifestSynchronizer()->uploadToRemote(manifest);

        LOG_DEBUG(log, "Manifest modified successfully for operation: {}, new version: {}", operation_name, manifest->version);
    }
    catch (const Exception & e)
    {
        /// If the exception is METADATA_CENTRALIZATION_BOSS_ERROR, mark Boss as unavailable
        if (e.code() == ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR)
        {
            LOG_ERROR(log, "Boss service error in syncMetadataFromBoss: {}", e.message());
            manager->setBossUnavailable(e.message());
        }

        tryLogCurrentException(log, "syncMetadataFromBoss failed");
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to modify manifest for {}", operation_name));
        throw;
    }
}

void DDLCentralized::submitDatabaseToBoss(
    const String & database_name,
    const UUID & database_uuid,
    const String & database_sql)
{
    LOG_DEBUG(log, "Uploading database {} to Boss", backQuoteIfNeed(database_name));

    auto boss_client_ptr = manager->getBossClientPtr();
    if (!boss_client_ptr)
    {
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "BossClient is null, cannot download manifest");
    }

    String db_uuid_str = toString(database_uuid);
    ManifestPtr manifest = manager->getManifestCache()->cloneManifestWithoutEtag();

    modifyManifest("upload database", [&]() {
        auto existing_db_opt = manifest->findDatabase(db_uuid_str);
        UInt32 new_version = existing_db_opt ? existing_db_opt->get().version + 1 : 1;

        String db_key = getDatabaseBossKey(database_name, new_version);
        boss_client_ptr->upload(db_key, database_sql);
        LOG_DEBUG(log, "Uploaded database SQL to Boss: {}", db_key);

        Database db_entry;
        db_entry.uuid = db_uuid_str;
        db_entry.key = db_key;
        db_entry.name = database_name;
        db_entry.version = new_version;
        db_entry.last_modified = getCurrentTimestamp();

        if (existing_db_opt)
        {
            db_entry.tables = existing_db_opt->get().tables;
            manifest->updateDatabase(db_uuid_str, db_entry);
        }
        else
            manifest->addDatabase(db_entry);
    }, manifest);

    LOG_INFO(log, "Database {} uploaded to Boss successfully", backQuoteIfNeed(database_name));
}

void DDLCentralized::submitTableToBoss(
    const String & database_name,
    const String & table_name,
    const UUID & database_uuid,
    const UUID & table_uuid,
    const String & table_sql)
{
    LOG_DEBUG(log, "Uploading table {}.{} to Boss", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    auto boss_client_ptr = manager->getBossClientPtr();
    if (!boss_client_ptr)
    {
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR,
            "BossClient is null, cannot download manifest");
    }

    String db_uuid_str = toString(database_uuid);
    String table_uuid_str = toString(table_uuid);
    ManifestPtr manifest = manager->getManifestCache()->cloneManifestWithoutEtag();

    modifyManifest("upload table", [&]() {
        auto db = findDatabaseOrThrow(manifest, database_uuid, database_name);

        auto existing_table_opt = db.get().findTable(table_uuid_str);
        UInt32 new_version = existing_table_opt ? existing_table_opt->get().version + 1 : 1;

        String table_key = getTableBossKey(db_uuid_str, table_name, new_version);
        boss_client_ptr->upload(table_key, table_sql);
        LOG_DEBUG(log, "Uploaded table SQL to Boss: {}", table_key);

        Table table_entry;
        table_entry.uuid = table_uuid_str;
        table_entry.key = table_key;
        table_entry.name = table_name;
        table_entry.version = new_version;
        table_entry.last_modified = getCurrentTimestamp();

        if (existing_table_opt)
            db.get().updateTable(table_uuid_str, table_entry);
        else
            db.get().addTable(table_entry);

        db.get().last_modified = getCurrentTimestamp();
    }, manifest);

    LOG_INFO(log, "Table {}.{} uploaded to Boss successfully", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
}

void DDLCentralized::syncMetadataOfAllNodes(bool drop_sync)
{
    if (manager->getConfig().cluster_name.empty())
    {
        LOG_DEBUG(log, "cluster_name not configured, skipping cluster notification");
        return;
    }

    try
    {
        const String & cluster_name = manager->getConfig().cluster_name;
        LOG_DEBUG(log, "Notifying cluster nodes in cluster: {} (drop_sync={})", cluster_name, drop_sync);

        auto cluster = getContext()->getCluster(cluster_name);
        if (!cluster)
        {
            LOG_WARNING(log, "Cluster {} not found, skipping notification", cluster_name);
            return;
        }

        auto query_context = Context::createCopy(getContext());
        query_context->setSetting("distributed_ddl_task_timeout", manager->getConfig().distributed_ddl_task_timeout);
        query_context->setCurrentQueryId("");

        /// Pass drop_sync parameter to syncMetadata function
        String notification_query = fmt::format(
            "SELECT hostname(), syncMetadata({}) FROM clusterAllReplicas('{}') settings skip_unavailable_shards = 1",
            drop_sync ? 1 : 0,
            cluster_name);

        LOG_DEBUG(log, "Executing notification query on cluster {}: {}", cluster_name, notification_query);

        BlockIO io = executeQuery(notification_query, query_context, QueryFlags{ .internal = true }).second;

        PullingPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
            if (block)
            {
                size_t rows = block.rows();
                if (rows > 0)
                {
                    const auto & columns = block.getColumnsWithTypeAndName();
                    for (const auto & column : columns)
                    {
                        for (size_t i = 0; i < rows; ++i)
                        {
                            Field field;
                            column.column->get(i, field);
                            LOG_DEBUG(log, "Query row {} result: {} = {}", i, column.name, field.dump());
                        }
                    }
                }
            }
        }

        LOG_INFO(log, "Successfully notified all nodes in cluster {} with drop_sync={}", cluster_name, drop_sync);
    }
    catch (...)  // NOLINT(bugprone-empty-catch)
    {
        tryLogCurrentException(log, "Failed to notify cluster nodes");
    }
}

void DDLCentralized::executeDropTable(
    const String & database_name,
    const String & table_name,
    bool if_exists,
    bool drop_sync)
{
    LOG_INFO(log, "Executing centralized DROP TABLE for {}.{} (drop_sync={})", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), drop_sync);
    String operation_description = fmt::format("DROP TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));

    try
    {
        checkBossAvailableFlag(operation_description);

        {
            auto lock = manager->acquireDistLock(operation_description);

            UUID table_uuid;
            syncBeforeOperation(fmt::format("DROP TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));

            auto drop_info = validateAndPrepareDropTable(database_name, table_name, if_exists);

            if (drop_info.database_uuid == UUIDHelpers::Nil || drop_info.table_uuid == UUIDHelpers::Nil)
            {
                LOG_WARNING(log, "DROP TABLE {}.{} skipped (database or table doesn't exist, if_exists=true)", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
                return;
            }

            table_uuid = drop_info.table_uuid;

            ManifestPtr manifest = manager->getManifestCache()->cloneManifestWithoutEtag();
            auto db_ref = findDatabaseOrThrow(manifest, drop_info.database_uuid, database_name);

            LOG_DEBUG(
                log,
                "Removing table {}.{} with database_uuid={}, table_uuid={} from manifest",
                backQuoteIfNeed(database_name),
                backQuoteIfNeed(table_name),
                toString(drop_info.database_uuid),
                toString(table_uuid));

            modifyManifest(
                fmt::format("DROP TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)),
                [&]()
                {
                    db_ref.get().removeTable(toString(table_uuid));
                    db_ref.get().last_modified = getCurrentTimestamp();
                },
                manifest);
        }

        LOG_DEBUG(log, "Syncing metadata from Boss after {} drop_sync {}", operation_description, drop_sync);
        syncMetadataOfAllNodes(drop_sync);

        LOG_INFO(log, "DROP TABLE {}.{} completed successfully", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to execute DROP TABLE {}.{}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name)));
        throw;
    }
}

void DDLCentralized::validateTableCreation(
    const DatabasePtr & database,
    const String & database_name,
    const String & table_name,
    const ASTCreateQuery & create) const
{
    if (database->isTableExist(table_name, getContext()))
    {
        if (create.if_not_exists)
        {
            LOG_WARNING(log, "Table {}.{} already exists, skipping creation (if_not_exists=true)",
                       backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
        }

        throw Exception(
            ErrorCodes::TABLE_ALREADY_EXISTS,
            "Table {}.{} already exists",
            backQuoteIfNeed(database_name),
            backQuoteIfNeed(table_name));
    }

    try
    {
        database->checkMetadataFilenameAvailability(table_name);
    }
    catch (const Exception &)
    {
        if (create.if_not_exists)
            throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Table {}.{} metadata file already exists",
                          backQuoteIfNeed(database_name), backQuoteIfNeed(table_name));
        throw;
    }

    database->checkTableNameLength(table_name);
}

void DDLCentralized::validateTableDataPath(
    const String & data_path,
    const ASTCreateQuery & create) const
{
    if (create.attach || data_path.empty())
        return;

    auto full_data_path = fs::path{getContext()->getPath()} / data_path;
    if (fs::exists(full_data_path))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS,
            "Directory for table data {} already exists", String(data_path));
}

void DDLCentralized::validateTableLimit(const ASTCreateQuery & create) const
{
    auto check_and_throw = [&](auto setting, CurrentMetrics::Metric metric, String setting_name, String entity_name)
    {
        UInt64 num_limit = getContext()->getGlobalContext()->getServerSettings()[setting];
        UInt64 attached_count = CurrentMetrics::get(metric);
        if (num_limit > 0 && attached_count >= num_limit)
            throw Exception(ErrorCodes::TOO_MANY_TABLES,
                            "Too many {}. "
                            "The limit (server configuration parameter `{}`) is set to {}, the current number is {}",
                            entity_name, setting_name, num_limit, attached_count);
    };

    String engine_name = create.storage && create.storage->engine ? create.storage->engine->name : "";
    bool is_replicated = engine_name.starts_with("Replicated") && engine_name.ends_with("MergeTree");

    if (is_replicated)
        check_and_throw(ServerSetting::max_replicated_table_num_to_throw,
                       CurrentMetrics::AttachedReplicatedTable,
                       "max_replicated_table_num_to_throw",
                       "replicated tables");
    else
        check_and_throw(ServerSetting::max_table_num_to_throw,
                       CurrentMetrics::AttachedTable,
                       "max_table_num_to_throw",
                       "tables");
}

void DDLCentralized::validateVirtualColumns(ASTCreateQuery & create)
{
    const String & engine_name = create.storage->engine->name;
    bool is_merge_tree = engine_name.ends_with("MergeTree");
    if (!is_merge_tree)
    {
        LOG_DEBUG(log, "Skipping virtual columns validation for {} engine", engine_name);
        return;
    }

    ColumnsDescription columns;
    if (create.columns_list && create.columns_list->columns)
        columns = InterpreterCreateQuery::getColumnsDescription(*create.columns_list->columns, getContext(), LoadingStrictnessLevel::CREATE);

    const auto & columns_all = columns.getAll();
    for (const auto & column : columns_all)
    {
        if (column.name == "_row_exists" || column.name == "_block_number" || column.name == "_block_offset")
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Cannot create table with column '{}' for MergeTree engines because it is reserved for persistent virtual column", column.name);
    }
}

DDLCentralized::TableUploadInfo DDLCentralized::prepareTableForUpload(
    const String & database_name,
    const String & table_name,
    ASTCreateQuery & create)
{
    if (!create.as_table.empty())
    {
        ContextMutablePtr mutable_context = std::const_pointer_cast<Context>(getContext());
        InterpreterCreateQuery(create.shared_from_this(), mutable_context)
            .getTablePropertiesAndNormalizeCreateQuery(create, LoadingStrictnessLevel::CREATE);
        
        /// Throw an exception if the table engine is not Distributed or MergeTreeFamily (MergeTree, ReplicatedMergeTree, ReplicatedAggregatingMergeTree, etc.)
        String engine_name = create.storage->engine->name;
        if (!engine_name.ends_with("MergeTree") && engine_name != "Distributed")
            throw Exception(
                ErrorCodes::NOT_IMPLEMENTED,
                "Table engine {} is not supported with metadata centralization. Only Distributed and MergeTree family engines are "
                "allowed.",
                engine_name);
    }

    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    UUID database_uuid = database->getUUID();

    validateTableCreation(database, database_name, table_name, create);

    String data_path = database->getTableDataPath(create);
    validateTableDataPath(data_path, create);

    TemporaryLockForUUIDDirectory uuid_lock{prepareTable(create, database_name, table_name)};

    validateTableLimit(create);

    validateVirtualColumns(create);

    String table_sql = generateSQL(create);

    return TableUploadInfo{database_uuid, create.uuid, table_sql};
}

DDLCentralized::DropTableInfo DDLCentralized::validateAndPrepareDropTable(
    const String & database_name,
    const String & table_name,
    bool if_exists)
{
    StorageID table_id(database_name, table_name);
    auto [database, table] = if_exists ? DatabaseCatalog::instance().tryGetDatabaseAndTable(table_id, getContext())
                                            : DatabaseCatalog::instance().getDatabaseAndTable(table_id, getContext());

    if (!table)
        return DropTableInfo{UUIDHelpers::Nil, UUIDHelpers::Nil, table_id};

    table_id.uuid = database->tryGetTableUUID(table_name);

    AccessFlags drop_storage = AccessType::DROP_TABLE;
    getContext()->checkAccess(drop_storage, table_id);
    LOG_DEBUG(log, "Access check passed for DROP TABLE {}.{}", backQuoteIfNeed(table_id.database_name), backQuoteIfNeed(table_id.table_name));

    table->checkTableCanBeDropped(getContext());

    bool check_ref_deps = getContext()->getSettingsRef()[Setting::check_referential_table_dependencies];
    bool check_loading_deps = !check_ref_deps && getContext()->getSettingsRef()[Setting::check_table_dependencies];
    DatabaseCatalog::instance().checkTableCanBeRemovedOrRenamed(table_id, check_ref_deps, check_loading_deps, false);

    return DropTableInfo{table_id.uuid, database->getUUID(), table_id};
}

DDLCentralized::DropDatabaseInfo DDLCentralized::validateAndPrepareDropDatabase(const String & database_name, bool if_exists)
{
    auto database
        = if_exists ? DatabaseCatalog::instance().tryGetDatabase(database_name) : DatabaseCatalog::instance().getDatabase(database_name);

    if (!database)
        return DropDatabaseInfo{UUIDHelpers::Nil, {}};

    // Check access rights
    getContext()->checkAccess(AccessType::DROP_DATABASE, database_name);
    LOG_DEBUG(log, "Access check passed for DROP DATABASE {}", backQuoteIfNeed(database_name));

    UUID database_uuid = database->getUUID();

    // Collect all tables in the database
    std::vector<std::pair<String, UUID>> tables;
    auto table_context = Context::createCopy(getContext());
    table_context->setInternalQuery(true);

    for (auto it = database->getTablesIterator(table_context); it->isValid(); it->next())
    {
        auto table_ptr = it->table();

        if (!table_ptr)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "While iterating database {}, found table {} without storage instance",
                backQuoteIfNeed(it->name()),
                backQuoteIfNeed(database_name));

        StorageID storage_id = table_ptr->getStorageID();
        String table_name = storage_id.table_name;
        UUID table_uuid = database->tryGetTableUUID(table_name);

        if (table_uuid != UUIDHelpers::Nil)
        {
            tables.emplace_back(table_name, table_uuid);
            LOG_DEBUG(log, "Found table {}.{} with UUID {}", backQuoteIfNeed(database_name), backQuoteIfNeed(table_name), toString(table_uuid));
        }
    }

    LOG_INFO(log, "Database {} has {} tables to drop", backQuoteIfNeed(database_name), tables.size());

    return DropDatabaseInfo{database_uuid, std::move(tables)};
}

void DDLCentralized::executeDropDatabase(const String & database_name, bool if_exists, bool drop_sync)
{
    LOG_INFO(log, "Executing centralized DROP DATABASE for {} (drop_sync={})", backQuoteIfNeed(database_name), drop_sync);
    String operation_description = fmt::format("DROP DATABASE {}", backQuoteIfNeed(database_name));

    try
    {
        checkBossAvailableFlag(operation_description);

        {
            auto lock = manager->acquireDistLock(operation_description);

            DropDatabaseInfo drop_info;
            syncBeforeOperation(fmt::format("DROP DATABASE {}", backQuoteIfNeed(database_name)));

            drop_info = validateAndPrepareDropDatabase(database_name, if_exists);

            if (drop_info.database_uuid == UUIDHelpers::Nil)
            {
                LOG_WARNING(log, "DROP DATABASE {} skipped (database doesn't exist, if_exists=true)", backQuoteIfNeed(database_name));
                return;
            }

            String db_uuid_str = toString(drop_info.database_uuid);

            LOG_DEBUG(log, "Removing database {} with UUID {} from manifest", backQuoteIfNeed(database_name), db_uuid_str);

            ManifestPtr manifest = manager->getManifestCache()->cloneManifestWithoutEtag();
            modifyManifest(fmt::format("DROP DATABASE {}", backQuoteIfNeed(database_name)), [&]() { manifest->removeDatabase(db_uuid_str); }, manifest);
        }
        
        LOG_DEBUG(log, "Syncing metadata from Boss after {} drop_sync {}", operation_description, drop_sync);
        syncMetadataOfAllNodes(drop_sync);

        LOG_INFO(log, "DROP DATABASE {} completed successfully", backQuoteIfNeed(database_name));
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("Failed to execute DROP DATABASE {}", backQuoteIfNeed(database_name)));
        throw;
    }
}

}
