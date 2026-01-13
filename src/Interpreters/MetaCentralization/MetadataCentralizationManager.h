#pragma once

#include <memory>
#include <mutex>
#include <unordered_set>

#include <Common/logger_useful.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/MetaCentralization/DistributedLock.h>
#include <Interpreters/MetaCentralization/ManifestSynchronizer.h>
#include <Interpreters/MetaCentralization/MetadataConfig.h>

namespace DB
{

class BossClient;
using BossClientPtr = std::shared_ptr<BossClient>;

class ManifestCache;
using ManifestCachePtr = std::unique_ptr<ManifestCache>;

class MetadataApplicator;
using MetadataApplicatorPtr = std::unique_ptr<MetadataApplicator>;

class UpdateOperationExecutor;
using UpdateOperationExecutorPtr = std::unique_ptr<UpdateOperationExecutor>;

class MetadataSyncTask;
using MetadataSyncTaskPtr = std::unique_ptr<MetadataSyncTask>;

class BossServiceRecoveryTask;
using BossServiceRecoveryTaskPtr = std::unique_ptr<BossServiceRecoveryTask>;

class HistoryCleanupTask;
using HistoryCleanupTaskPtr = std::unique_ptr<HistoryCleanupTask>;

struct Manifest;
using ManifestPtr = std::shared_ptr<Manifest>;

using LocalLockPtr = std::unique_lock<std::timed_mutex>;

/// Manages centralized metadata synchronization with remote Boss storage.
/// Coordinates manifest synchronization, cache management, and metadata application.
class MetadataCentralizationManager : public WithContext
{
public:
    explicit MetadataCentralizationManager(const MetadataCentralizationConfig & config_, ContextMutablePtr context_);

    ~MetadataCentralizationManager();

    /// Update metadata from Boss manifest, return true if successful
    bool syncMetadataFromBoss(bool drop_sync = false);

    /// Initialize metadata on server startup
    void initializeOnStartup();

    /// Initialize manifest in Boss storage if not exists
    /// Returns true if successful, false otherwise
    bool initializeBossManifest();

    /// Get Boss client pointer
    BossClientPtr getBossClientPtr() const;

    /// Reinitialize Boss client (called by recovery task)
    /// Returns true if successful, false otherwise
    /// @param force If true, recreate the client even if it already exists
    bool reinitializeBossClient(bool force = false);

    /// Get configuration
    const MetadataCentralizationConfig & getConfig() const;
    const ManifestSynchronizerPtr & getManifestSynchronizer() const { return manifest_synchronizer; }

    const ManifestCachePtr & getManifestCache() const { return manifest_cache; }

    /// Start background synchronization and boss recovery task
    void startTasks();

    /// Stop background synchronization and boss recovery task
    void stopTasks();

    /// Get Boss service availability flag
    bool getBossAvailableFlag() const;

    /// Probe Boss service health by making actual service calls
    void probeBossService(bool & is_service_active, bool & has_manifest) const;

    /// Get Boss last error message
    String getBossLastError() const;

    /// Set Boss service as unavailable
    void setBossUnavailable(const std::string & error_msg = "");

    /// Set Boss service as available (called by recovery task)
    void setBossAvailable();

    void shutdown();

    DistributedLockPtr acquireDistLock(const String & operation_description);

    bool isSystemDatabase(const String & database_name) const
    {
        return system_databases.contains(database_name);
    }

private:
    std::timed_mutex local_mutex;
    MetadataCentralizationConfig config;

    String boss_endpoint;
    String boss_access_key;
    String boss_secret_key;
    String boss_region;

    mutable std::mutex boss_client_mutex;
    BossClientPtr boss_client_ptr;

    std::atomic_bool boss_is_available{true};
    mutable std::mutex boss_error_mutex;
    String boss_last_error;

    std::atomic<bool> is_shutdown{false};

    ManifestCachePtr manifest_cache;
    ManifestSynchronizerPtr manifest_synchronizer;
    MetadataApplicatorPtr applicator;
    UpdateOperationExecutorPtr operation_executor;

    BossServiceRecoveryTaskPtr recovery_task;
    MetadataSyncTaskPtr sync_task;
    HistoryCleanupTaskPtr cleanup_task;

    LocalLockPtr acquireLocalLock(const String & operation_description);

    LoggerPtr log;

    std::unordered_set<String> system_databases = {"system", "default", "INFORMATION_SCHEMA", "information_schema"};
};

using MetadataCentralizationManagerPtr = std::shared_ptr<MetadataCentralizationManager>;

}
