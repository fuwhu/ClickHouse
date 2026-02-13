#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>

#include <Databases/IDatabase.h>
#include <IO/Boss/BossClient.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/MetaCentralization/BossServiceRecoveryTask.h>
#include <Interpreters/MetaCentralization/HistoryCleanupTask.h>
#include <Interpreters/MetaCentralization/ManifestCache.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/ManifestSynchronizer.h>
#include <Interpreters/MetaCentralization/MetadataApplicator.h>
#include <Interpreters/MetaCentralization/MetadataSyncTask.h>
#include <Interpreters/MetaCentralization/UpdateOperationExecutor.h>
#include <Interpreters/StorageID.h>
#include <Parsers/ASTCreateQuery.h>
#include <Storages/IStorage.h>
#include <Common/CurrentMetrics.h>
#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>

namespace CurrentMetrics
{
    extern const Metric MetaCentraBossManifestVersion;
    extern const Metric MetaCentraBossDatabaseCount;
    extern const Metric MetaCentraBossTableCount;
    extern const Metric MetaCentraLocalManifestVersion;
    extern const Metric MetaCentraLocalDatabaseCount;
    extern const Metric MetaCentraLocalTableCount;
    extern const Metric MetaCentraSyncFromBossStatus;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_CENTRALIZATION_LOCAL_LOCK_TIMEOUT;
    extern const int METADATA_CENTRALIZATION_DIST_LOCK_TIMEOUT;
    extern const int LOGICAL_ERROR;
    extern const int METADATA_CENTRALIZATION_BOSS_ERROR;
    extern const int METADATA_CENTRALIZATION_SERVER_ERROR;
}

MetadataCentralizationManager::MetadataCentralizationManager(const MetadataCentralizationConfig & config_, ContextMutablePtr context_)
    : WithContext(context_)
    , config(config_)
    , boss_endpoint(config_.endpoint)
    , boss_access_key(config_.access_key_id)
    , boss_secret_key(config_.secret_access_key)
    , boss_region(config_.region)
    , log(getLogger("MetadataCentralizationManager"))
{
    config.validate();

    /// Initialize BossClient - may throw on failure
    try
    {
        boss_client_ptr = std::make_shared<BossClient>(boss_endpoint, boss_access_key, boss_secret_key, boss_region, config.boss_max_redirects, config.boss_retry_attempts);
        LOG_INFO(log, "BossClient initialized successfully");
    }
    catch (const Exception & e)
    {
        String error_msg = fmt::format("BossClient initialization failed: {}", e.message());
        LOG_WARNING(log, "{}", error_msg);
        LOG_WARNING(log, "ClickHouse will start with Boss service unavailable. Recovery task will attempt to reconnect.");
        setBossUnavailable(error_msg);
    }
    catch (...)
    {
        String error_msg = "BossClient initialization failed with unknown error";
        LOG_WARNING(log, "{}", error_msg);
        LOG_WARNING(log, "ClickHouse will start with Boss service unavailable. Recovery task will attempt to reconnect.");
        setBossUnavailable(error_msg);
    }

    manifest_cache = std::make_unique<ManifestCache>();
    manifest_synchronizer = std::make_unique<ManifestSynchronizer>(this, getContext());
    applicator = std::make_unique<MetadataApplicator>(this, context_);
    operation_executor = std::make_unique<UpdateOperationExecutor>(this, applicator, getContext());
    recovery_task = std::make_unique<BossServiceRecoveryTask>(getContext(), this, config.recovery_interval_seconds);
    sync_task = std::make_unique<MetadataSyncTask>(getContext(), this, config.sync_interval_seconds);
    cleanup_task = std::make_unique<HistoryCleanupTask>(getContext(), this, config.cleanup_markers_path, config.cleanup_interval_seconds, config.cleanup_retention_days);
}

void MetadataCentralizationManager::shutdown()
{
    if (is_shutdown.exchange(true))
        return;

    LOG_INFO(log, "Shutting down metadata centralization manager");
    stopTasks();
}

MetadataCentralizationManager::~MetadataCentralizationManager()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in MetadataCentralizationManager destructor");
    }
}

bool MetadataCentralizationManager::syncMetadataFromBoss(bool drop_sync)
{
    LOG_DEBUG(log, "Starting syncMetadataFromBoss with drop_sync={}", drop_sync);

    auto lock = acquireLocalLock("syncMetadataFromBoss");

    try
    {
        const auto & boss_manifest = manifest_synchronizer->downloadFromRemote();

        const String & boss_etag = boss_manifest->etag;

        LOG_DEBUG(log, "Downloaded manifest from Boss, etag: {}, databases: {}", boss_etag, boss_manifest->databases.size());

        CurrentMetrics::set(CurrentMetrics::MetaCentraBossManifestVersion, boss_manifest->version);

        if (manifest_cache->getEtag() == boss_etag)
        {
            LOG_DEBUG(log, "Etag matches ({}), skipping update", boss_etag);
            return true;
        }

        LOG_INFO(log, "Etag mismatch (local: {}, boss: {}), need to sync metadata", manifest_cache->getEtag(), boss_etag);

        std::vector<UpdateOperationExecutor::Operation> operations = operation_executor->planUpdates(boss_manifest, drop_sync);

        LOG_INFO(log, "Planned {} update operations", operations.size());

        std::vector<UpdateOperationExecutor::Result> results;
        results.reserve(operations.size());

        for (const auto & op : operations)
            results.push_back(operation_executor->executeUpdate(op));

        size_t total_ops = results.size();
        size_t successful_ops = 0;
        size_t failed_db_ops = 0;
        size_t failed_table_ops = 0;

        for (const auto & result : results)
        {
            if (result.success)
                successful_ops++;
            else
            {
                if (result.operation.type == UpdateOperationExecutor::OperationType::CREATE_DATABASE
                    || result.operation.type == UpdateOperationExecutor::OperationType::DROP_DATABASE)
                    failed_db_ops++;
                else
                    failed_table_ops++;

                LOG_ERROR(
                    log,
                    "Failed operation: type={}, db={}, table={}, error={}",
                    static_cast<int>(result.operation.type),
                    result.operation.database_name,
                    result.operation.table_name,
                    result.error_message);
            }
        }

        LOG_INFO(
            log,
            "Update results: total={}, success={}, failed_db={}, failed_table={}",
            total_ops,
            successful_ops,
            failed_db_ops,
            failed_table_ops);

        operation_executor->applyCacheUpdates(results);

        bool all_success = (successful_ops == total_ops);

        if (all_success)
        {
            LOG_INFO(log, "All updates successful, updating manifest and persisting to disk");
            manifest_cache->updateEtag(boss_etag);
            manifest_cache->updateVersion(boss_manifest->version);
            manifest_cache->updateLastModified(boss_manifest->last_modified);
            manifest_synchronizer->persistToLocal(boss_manifest, boss_etag);
            LOG_INFO(log, "syncMetadataFromBoss completed successfully, new etag: {}, version: {}", boss_etag, boss_manifest->version);

            CurrentMetrics::set(CurrentMetrics::MetaCentraLocalManifestVersion, boss_manifest->version);
            CurrentMetrics::set(CurrentMetrics::MetaCentraSyncFromBossStatus, 0);
        }
        else
        {
            if (successful_ops > 0) 
            {
                CurrentMetrics::set(CurrentMetrics::MetaCentraSyncFromBossStatus, 2);
                String uuid = toString(UUIDHelpers::generateV4());
                LOG_WARNING(log, "Partial update success, manifest persisted to disk with uuid: {}", uuid);
                LOG_WARNING(log, "Failed: {} database ops, {} table ops", failed_db_ops, failed_table_ops);
                manifest_cache->updateEtag(uuid);
                manifest_cache->updateLastModified(boss_manifest->last_modified);
                manifest_synchronizer->persistToLocal(manifest_cache->cloneManifestWithoutEtag(), uuid);
            }
            else
            {
                CurrentMetrics::set(CurrentMetrics::MetaCentraSyncFromBossStatus, 3);
                LOG_WARNING(log, "All updates failed, manifest not persisted to disk");
            }
        }

        CurrentMetrics::set(CurrentMetrics::MetaCentraBossDatabaseCount, boss_manifest->databases_count);
        CurrentMetrics::set(CurrentMetrics::MetaCentraBossTableCount, boss_manifest->getTablesCount());
        CurrentMetrics::set(CurrentMetrics::MetaCentraLocalDatabaseCount, manifest_cache->getAllDatabases().size());
        CurrentMetrics::set(CurrentMetrics::MetaCentraLocalTableCount, manifest_cache->getAllTables().size());

        return all_success;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR)
        {
            LOG_ERROR(log, "Boss service error in syncMetadataFromBoss: {}", e.message());
            CurrentMetrics::set(CurrentMetrics::MetaCentraSyncFromBossStatus, 1);
            setBossUnavailable(e.message());
        }

        tryLogCurrentException(log, "syncMetadataFromBoss failed");
        throw;
    }
    catch (...)
    {
        tryLogCurrentException(log, "syncMetadataFromBoss failed");
        throw;
    }
}

void MetadataCentralizationManager::initializeOnStartup()
{
    LOG_INFO(log, "Initializing metadata centralization on startup");

    try
    {
        if (!manifest_synchronizer->localManifestExists() && config.generate_local_manifest_from_local_metadata)
            generateLocalManifest(true);
        
        if (!manifest_synchronizer->localManifestExists())
        {
            auto & catalog = DatabaseCatalog::instance();
            const auto & databases = catalog.getDatabases();

            for (const auto & [database_name, db] : databases)
            {
                if (!isSystemDatabase(database_name))
                {
                    throw Exception(
                        ErrorCodes::METADATA_CENTRALIZATION_SERVER_ERROR,
                        "Cannot start server with metadata centralization enabled: local manifest.json is missing "
                        "and non-system database '{}' exists. Enable generate_local_manifest_from_local_metadata to genreate local "
                        "manifest from local metadata, or ensure local manifest exists before starting.",
                        database_name);
                }
            }

            generateLocalManifest(false);
        }

        LOG_INFO(log, "Loading local manifest");
        ManifestPtr local_manifest = manifest_synchronizer->loadFromLocal();
        manifest_cache->load(local_manifest);

        CurrentMetrics::set(CurrentMetrics::MetaCentraLocalManifestVersion, local_manifest->version);
        CurrentMetrics::set(CurrentMetrics::MetaCentraLocalDatabaseCount, manifest_cache->getAllDatabases().size());
        CurrentMetrics::set(CurrentMetrics::MetaCentraLocalTableCount, manifest_cache->getAllTables().size());

        /// Check Boss service status
        bool is_service_active = false;
        bool has_manifest = false;
        probeBossService(is_service_active, has_manifest);

        if (!is_service_active)
        {
            if (config.initialize_centralized_metadata)
            {
                throw Exception(
                    ErrorCodes::METADATA_CENTRALIZATION_SERVER_ERROR,
                    "Cannot start server with metadata centralization enabled: initialize_centralized_metadata is enabled, but Boss service is not available.");
            }

            LOG_WARNING(log, "Boss service is not available, starting with local metadata manifest_cache only. DDL will be disabled.");
            setBossUnavailable("Boss service check failed during startup");
            return;
        }

        LOG_INFO(log, "Boss service is available");

        if (!has_manifest)
        {
            if (!config.initialize_centralized_metadata)
            {
                LOG_ERROR(log, "No manifest found in Boss and initial_boss_manifest is disabled. Setting Boss service as unavailable.");
                setBossUnavailable("No Boss manifest exists and initial_boss_manifest configuration is disabled");
                return;
            }

            LOG_WARNING(log, "No manifest found in Boss, will initialize");
            
            initializeCentralizedMetadata();
        }
        else
        {
            if (config.initialize_centralized_metadata)
            {
                throw Exception(
                    ErrorCodes::METADATA_CENTRALIZATION_SERVER_ERROR,
                    "Cannot start server with metadata centralization enabled: initialize_centralized_metadata is enabled, but Boss manifest already exists. "
                    "Please disable it to continue.");
            }
        }

        LOG_INFO(log, "Syncing metadata from Boss");
        syncMetadataFromBoss();

        LOG_INFO(log, "Metadata centralization initialized successfully");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to initialize metadata centralization");
        throw;
    }
}

void MetadataCentralizationManager::startTasks()
{
    /// Start recovery task
    if (recovery_task)
    {
        LOG_INFO(log, "Starting Boss service recovery task");
        recovery_task->start();
    }

    /// Then start sync task
    if (sync_task)
    {
        LOG_INFO(log, "Starting metadata sync task");
        sync_task->start();
    }

    /// Finally start cleanup task
    if (cleanup_task)
    {
        LOG_INFO(log, "Starting history cleanup task");
        cleanup_task->start();
    }
}

void MetadataCentralizationManager::stopTasks()
{
    if (cleanup_task)
    {
        LOG_INFO(log, "Stopping history cleanup task");
        cleanup_task->shutdown();
    }

    if (sync_task)
    {
        LOG_INFO(log, "Stopping metadata sync task");
        sync_task->shutdown();
    }

    if (recovery_task)
    {
        LOG_INFO(log, "Stopping Boss service recovery task");
        recovery_task->shutdown();
    }
}

BossClientPtr MetadataCentralizationManager::getBossClientPtr() const
{
    std::lock_guard<std::mutex> lock(boss_client_mutex);
    return boss_client_ptr;
}

const MetadataCentralizationConfig & MetadataCentralizationManager::getConfig() const
{
    return config;
}

void MetadataCentralizationManager::generateLocalManifest(bool from_local_metadata)
{
    LOG_INFO(log, "Generating local manifest, from_local_metadata: {}", from_local_metadata);

    try
    {
        ManifestPtr manifest = std::make_shared<Manifest>();
        String etag = toString(UUIDHelpers::generateV4());
        LOG_INFO(log, "Creating local manifest with etag {}", etag);
        manifest->etag = etag;
        manifest->version = 1;
        manifest->ck_version = String(VERSION_STRING) + "-" + String(BILI_VERSION_STRING);

        time_t now = time(nullptr);
        manifest->last_modified = DateLUT::instance().timeToString(now);

        if (from_local_metadata)
        {
            auto & catalog = DatabaseCatalog::instance();
            const auto & databases = catalog.getDatabases();

            for (const auto & [db_name, db_ptr] : databases)
            {
                if (!db_ptr)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected null database pointer for database {}", db_name);

                String db_engine = db_ptr->getEngineName();
                LOG_DEBUG(log, "Processing database: {} engine {}", db_name, db_engine);

                if (isSystemDatabase(db_name))
                {
                    LOG_DEBUG(log, "Skipping system database: {}", db_name);
                    continue;
                }

                if (db_engine != "Atomic")
                    throw Exception(ErrorCodes::METADATA_CENTRALIZATION_SERVER_ERROR, "Database {} is not an Atomic database", db_name);

                LOG_INFO(log, "Processing database: {}", db_name);

                /// Create database metadata
                Database db_metadata;
                db_metadata.uuid = toString(db_ptr->getUUID());
                db_metadata.name = db_name;
                db_metadata.version = 1;
                db_metadata.last_modified = manifest->last_modified;

                String db_key = fmt::format("{}_{}.sql", db_name, db_metadata.version);
                db_metadata.key = db_key;

                LOG_DEBUG(log, "Generated database key: {}", db_key);

                /// Process tables in this database
                for (auto table_it = db_ptr->getTablesIterator(getContext()); table_it->isValid(); table_it->next())
                {
                    const String & table_name = table_it->name();
                    auto table_ptr = table_it->table();

                    if (!table_ptr)
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "While iterating database {}, found table {} without storage instance",
                            db_name,
                            table_name);

                    String tb_engine_name = table_ptr->getName();
                    LOG_DEBUG(log, "Processing table: {}.{}, engine: {}", db_name, table_name, tb_engine_name);

                    if (!tb_engine_name.ends_with("MergeTree") && tb_engine_name != "Distributed")
                        throw Exception(
                            ErrorCodes::METADATA_CENTRALIZATION_SERVER_ERROR,
                            "Table {}.{} is not MergeTree Family or Distributed table",
                            db_name,
                            table_name);

                    Table table_metadata;
                    table_metadata.uuid = toString(table_ptr->getStorageID().uuid);
                    table_metadata.name = table_name;
                    table_metadata.version = 1;
                    table_metadata.last_modified = manifest->last_modified;

                    String table_key = fmt::format("{}/{}_{}.sql", db_metadata.uuid, table_name, table_metadata.version);
                    table_metadata.key = table_key;

                    LOG_DEBUG(log, "Generated table key: {}", table_key);

                    db_metadata.addTable(table_metadata);
                }

                db_metadata.tables_count = db_metadata.tables.size();

                manifest->addDatabase(db_metadata);

                LOG_INFO(log, "Added database {} with {} tables to manifest", db_name, db_metadata.tables_count);
            }
        }

        manifest->databases_count = manifest->databases.size();
        LOG_INFO(log, "Generated local manifest with {} databases", manifest->databases_count);

        manifest_synchronizer->persistToLocal(manifest, etag);
        LOG_INFO(log, "Successfully generated local manifest");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to initialize local manifest");
        throw;
    }
}

void MetadataCentralizationManager::initializeCentralizedMetadata()
{
    LOG_INFO(log, "Initializing centralized manifest");

    try
    {
        auto lock = acquireDistLock("Initializing centralized manifest");
        LOG_DEBUG(log, "Acquired distributed lock for manifest initialization");

        auto & catalog = DatabaseCatalog::instance();
        auto manifest = getManifestCache()->cloneManifestWithoutEtag();
        auto manifest_dbs = manifest->databases;

        for (auto & manifest_db : manifest_dbs)
        {
            String db_name = manifest_db.name;
            auto db_ptr = catalog.getDatabase(db_name);

            auto db_create_query = db_ptr->getCreateDatabaseQuery();
            auto & db_create = db_create_query->as<ASTCreateQuery &>();
            db_create.setDatabase(TABLE_WITH_UUID_NAME_PLACEHOLDER);
            db_create.attach = true;
            db_create.if_not_exists = false;

            WriteBufferFromOwnString db_statement_buf;
            IAST::FormatSettings format_settings(/*one_line=*/false, /*hilite=*/false);
            db_create.format(db_statement_buf, format_settings);
            writeChar('\n', db_statement_buf);
            String db_sql = db_statement_buf.str();
            if (!db_sql.empty() && db_sql.back() == '\n')
                db_sql.pop_back();

            String db_key = manifest_db.key;

            LOG_DEBUG(log, "Uploading database SQL to Boss: {}", db_key);
            boss_client_ptr->upload(db_key, db_sql);


            for (auto & manifest_tb : manifest_db.tables)
            {
                String table_name = manifest_tb.name;
                UUID table_uuid = MetadataApplicator::parseUUIDFromString(manifest_tb.uuid);
                StorageID table_id = StorageID(db_name, table_name, table_uuid);
                auto table_ptr = catalog.getTable(table_id, getContext());

                auto table_create_query = db_ptr->getCreateTableQuery(table_name, getContext());
                auto & table_create = table_create_query->as<ASTCreateQuery &>();
                table_create.database.reset();
                table_create.setTable(TABLE_WITH_UUID_NAME_PLACEHOLDER);
                table_create.attach = true;
                table_create.if_not_exists = false;

                WriteBufferFromOwnString table_statement_buf;
                table_create.format(table_statement_buf, format_settings);
                writeChar('\n', table_statement_buf);
                String table_sql = table_statement_buf.str();
                if (!table_sql.empty() && table_sql.back() == '\n')
                    table_sql.pop_back();

                String table_key = manifest_tb.key;

                LOG_DEBUG(log, "Uploading table SQL to Boss: {}", table_key);
                boss_client_ptr->upload(table_key, table_sql);
            }
        }

        LOG_DEBUG(log, "Uploading initial manifest to Boss");
        String etag = manifest_synchronizer->uploadToRemote(manifest);
        LOG_INFO(log, "Successfully uploaded manifest to Boss with etag: {}", etag);
        manifest_cache->updateEtag(etag);
        manifest_synchronizer->persistToLocal(manifest, etag);

        CurrentMetrics::set(CurrentMetrics::MetaCentraBossManifestVersion, manifest->version);
        CurrentMetrics::set(CurrentMetrics::MetaCentraBossDatabaseCount, manifest->databases_count);
        CurrentMetrics::set(CurrentMetrics::MetaCentraBossTableCount, manifest->getTablesCount());
        LOG_INFO(log, "Successfully initialized centralized manifest with etag: {}", etag);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to initialize centralized manifest");
        throw;
    }
}

bool MetadataCentralizationManager::getBossAvailableFlag() const
{
    return boss_is_available.load(std::memory_order_acquire);
}

void MetadataCentralizationManager::probeBossService(bool & is_service_active, bool & has_manifest) const
{
    is_service_active = false;
    has_manifest = false;
    try
    {
        ManifestPtr manifest = manifest_synchronizer->downloadFromRemote();
        LOG_DEBUG(log, "Successfully downloaded manifest from Boss");
        is_service_active = true;
        has_manifest = true;
    }
    catch (const Exception & e)
    {
        if (e.message().find("The specified key does not exist") != std::string::npos
        && e.message().find("while reading key:") != std::string::npos
        && e.message().find("manifest.json") != std::string::npos)
        {
            LOG_DEBUG(log, "Manifest not found in Boss, service is active but needs initialization");
            is_service_active = true;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to check Boss service status", LogsLevel::warning);
    }
}

String MetadataCentralizationManager::getBossLastError() const
{
    std::lock_guard<std::mutex> lock(boss_error_mutex);
    return boss_last_error;
}

void MetadataCentralizationManager::setBossUnavailable(const std::string & error_msg)
{
    bool old_val = true;
    bool became_unavailable = boss_is_available.compare_exchange_strong(old_val, false);

    if (became_unavailable) 
    {
        {
            std::lock_guard<std::mutex> lock(boss_client_mutex);
            boss_client_ptr.reset();
        }

        {
            std::lock_guard<std::mutex> lock(boss_error_mutex);
            if (!error_msg.empty())
                boss_last_error = error_msg;
        }

        if (recovery_task) 
            recovery_task->wakeup();
    }
    else if (!error_msg.empty()) 
    {
        std::lock_guard<std::mutex> lock(boss_error_mutex);
        boss_last_error = error_msg;
    }
}

void MetadataCentralizationManager::setBossAvailable()
{
    bool old_val = false;
    if (boss_is_available.compare_exchange_strong(old_val, true))
    {
        LOG_INFO(log, "Boss service is now AVAILABLE");

        {
            std::lock_guard<std::mutex> lock(boss_error_mutex);
            boss_last_error.clear();
        }

    }
}

bool MetadataCentralizationManager::reinitializeBossClient(bool force)
{
    LOG_INFO(log, "Attempting to reinitialize BossClient (force={})", force);

    try
    {
        {
            std::lock_guard<std::mutex> lock(boss_client_mutex);

            if (boss_client_ptr && !force)
            {
                LOG_DEBUG(log, "BossClient already initialized by another thread, skipping");
                return true;
            }

            if (boss_client_ptr && force)
            {
                LOG_INFO(log, "Force recreating BossClient due to repeated failures");
                boss_client_ptr.reset();
            }

            auto new_client = std::make_shared<BossClient>(boss_endpoint, boss_access_key, boss_secret_key, boss_region, config.boss_max_redirects, config.boss_retry_attempts);
            boss_client_ptr = std::move(new_client);
        }

        LOG_INFO(log, "BossClient reinitialized successfully");
        return true;
    }
    catch (const Exception & e)
    {
        String error_msg = fmt::format("BossClient reinitialize failed: {}", e.message());
        LOG_WARNING(log, "{}", error_msg);
        return false;
    }
    catch (...)
    {
        String error_msg = "BossClient reinitialize failed with unknown error";
        LOG_WARNING(log, "{}", error_msg);
        return false;
    }
}

LocalLockPtr MetadataCentralizationManager::acquireLocalLock(const String & operation_description)
{
    LOG_DEBUG(log, "Acquiring local lock for operation: {}", operation_description);

    if (!local_mutex.try_lock_for(config.local_lock_timeout_ms))
    {
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_LOCAL_LOCK_TIMEOUT,
            "Failed to acquire local lock for {} within {}ms",
            operation_description,
            config.local_lock_timeout_ms.count());
    }

    LOG_DEBUG(log, "Local lock acquired for operation: {}", operation_description);
    return LocalLockPtr(local_mutex, std::adopt_lock);
}

DistributedLockPtr MetadataCentralizationManager::acquireDistLock(const String & operation_description)
{
    LOG_DEBUG(log, "Acquiring distributed lock for operation: {}", operation_description);

    auto zookeeper = getContext()->getZooKeeper();

    auto distributed_lock = std::make_unique<DistributedLock>(
        zookeeper,
        config.lock_path,
        config.lock_key,
        config.dist_lock_timeout_ms);

    if (!distributed_lock->tryLock())
    {
        throw Exception(
            ErrorCodes::METADATA_CENTRALIZATION_DIST_LOCK_TIMEOUT,
            "Failed to acquire distributed lock for {} within {}ms",
            operation_description,
            config.dist_lock_timeout_ms.count());
    }

    LOG_DEBUG(log, "Distributed lock acquired for operation: {}", operation_description);

    return distributed_lock; 
}

}
