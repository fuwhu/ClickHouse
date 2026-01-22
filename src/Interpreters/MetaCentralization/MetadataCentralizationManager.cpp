#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>

#include <Common/DateLUTImpl.h>
#include <Common/Exception.h>
#include <Common/config_version.h>
#include <Common/logger_useful.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetaCentralization/ManifestCache.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/ManifestSynchronizer.h>
#include <Interpreters/MetaCentralization/MetadataApplicator.h>
#include <Interpreters/MetaCentralization/MetadataSyncTask.h>
#include <Interpreters/MetaCentralization/BossServiceRecoveryTask.h>
#include <Interpreters/MetaCentralization/HistoryCleanupTask.h>
#include <Interpreters/MetaCentralization/UpdateOperationExecutor.h>
#include <IO/Boss/BossClient.h>

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

        if (!manifest_cache->isLoaded())
        {
            LOG_DEBUG(log, "Cache not loaded, loading from local disk");
            ManifestPtr local_manifest = manifest_synchronizer->loadFromLocal();
            manifest_cache->load(local_manifest);
        }

        if (manifest_cache->isLoaded() && manifest_cache->getEtag() == boss_etag)
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
            manifest_cache->updateManifestWithEtag(boss_manifest, boss_etag);
            manifest_synchronizer->persistToLocal(boss_manifest, boss_etag);
            LOG_INFO(log, "syncMetadataFromBoss completed successfully, new etag: {}", boss_etag);
        }
        else
        {
            LOG_WARNING(log, "Partial update success, only manifest_cache mappings updated. Manifest and etag not changed.");
            LOG_WARNING(log, "Failed: {} database ops, {} table ops", failed_db_ops, failed_table_ops);
        }

        return all_success;
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::METADATA_CENTRALIZATION_BOSS_ERROR)
        {
            LOG_ERROR(log, "Boss service error in syncMetadataFromBoss: {}", e.message());
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
        /// Throw exception if metadata centralization is enabled while local manifest.json does not exist,
        /// and some non-system database exists in local server.
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
                        "Can not start server with metadata centralization enabled while local manifest.json is missing and non-system database {} exists in local server.",
                        database_name);
                }
            }
        }

        /// Check Boss service status
        bool is_service_active = false;
        bool has_manifest = false;
        probeBossService(is_service_active, has_manifest);

        if (!is_service_active)
        {
            LOG_WARNING(log, "Boss service is not available, starting with local metadata manifest_cache only. DDL will be disabled.");
            setBossUnavailable("Boss service check failed during startup");
            return;
        }

        LOG_INFO(log, "Boss service is available");

        if (!has_manifest)
        {
            LOG_WARNING(log, "No manifest found in Boss, will initialize");
            if (!initializeBossManifest())
            {
                LOG_ERROR(log, "Failed to initialize Boss manifest");
                return;
            }
        }
        else
        {
            LOG_INFO(log, "Syncing metadata from Boss");
            syncMetadataFromBoss();
        }

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

bool MetadataCentralizationManager::initializeBossManifest()
{
    LOG_INFO(log, "Initializing Boss manifest.json");

    if (manifest_synchronizer->localManifestExists())
    {
        LOG_ERROR(log, "Local manifest.json already exists, cannot initialize Boss manifest");
        setBossUnavailable("Local manifest.json already exists, cannot initialize Boss manifest");
        return false;
    }

    try
    {
        auto lock = acquireDistLock("Initializing Boss manifest");

        LOG_DEBUG(log, "Acquired distributed lock for manifest initialization");

        /// Check if manifest was already created by another node while we were waiting for the lock
        bool is_service_active = false;
        bool has_manifest = false;
        probeBossService(is_service_active, has_manifest);

        if (!is_service_active)
        {
            LOG_ERROR(log, "Boss service became unavailable during initialization");
            setBossUnavailable("Boss service check failed during manifest initialization");
            return false;
        }

        if (has_manifest)
        {
            LOG_INFO(log, "Manifest was already initialized by another node");
            return true;
        }

        /// Create new manifest
        ManifestPtr manifest = std::make_shared<Manifest>();
        manifest->ck_version = String(VERSION_STRING) + "-" + String(BILI_VERSION_STRING);

        time_t now = time(nullptr);
        manifest->last_modified = DateLUT::instance().timeToString(now);

        LOG_DEBUG(log, "Uploading initial manifest to Boss");

        String etag = manifest_synchronizer->uploadToRemote(manifest);

        LOG_INFO(log, "Successfully initialized Boss manifest with etag: {}", etag);

        manifest_synchronizer->persistToLocal(manifest, etag);

        manifest_cache->updateManifestWithEtag(manifest, etag);
        manifest_cache->load(manifest);

        LOG_INFO(log, "Boss manifest initialization completed");

        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to initialize Boss manifest");
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
