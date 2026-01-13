#pragma once

#include <atomic>
#include <memory>
#include <unordered_set>

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MetadataCentralizationManager;

/// Background task for cleaning up historical version files from Boss storage
/// Runs daily to remove old db_version.sql and tb_version.sql files
class HistoryCleanupTask : public WithContext
{
public:
    HistoryCleanupTask(
        ContextPtr context_,
        MetadataCentralizationManager * manager_,
        String cleanup_markers_path_,
        UInt32 cleanup_interval_seconds_,
        UInt32 cleanup_retention_days_);

    ~HistoryCleanupTask();

    void start();

    void shutdown();

private:
    void run();

    void threadFunction();

    /// Check if cleanup has been done today
    /// Returns true if cleanup was already completed today
    bool isCleanupDoneToday();

    /// Mark cleanup as done for today in ZooKeeper
    void markCleanupDone();

    /// Clean up old marker nodes in ZooKeeper (older than 180 days)
    void cleanupOldMarkers(zkutil::ZooKeeperPtr zookeeper, const String & marker_base_path);

    /// Execute the actual cleanup task
    /// Returns true if successful
    bool executeCleanup();

    /// Get list of version files that are referenced in manifest
    /// These files should be preserved
    std::unordered_set<String> getReferencedVersionFiles();

    ThreadFromGlobalPoolNoTracingContextPropagation cleanup_thread;

    MetadataCentralizationManager * manager;
    String cleanup_markers_path;
    UInt32 cleanup_interval_seconds;
    UInt32 cleanup_retention_days;

    std::atomic<bool> is_stopped{false};
    LoggerPtr log;
};

using HistoryCleanupTaskPtr = std::unique_ptr<HistoryCleanupTask>;
}
