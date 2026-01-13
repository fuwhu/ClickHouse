#include <Interpreters/MetaCentralization/HistoryCleanupTask.h>

#include <chrono>
#include <thread>
#include <unordered_set>
#include <unistd.h>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetaCentralization/ManifestCache.h>
#include <Interpreters/MetaCentralization/ManifestModel.h>
#include <Interpreters/MetaCentralization/ManifestSynchronizer.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <IO/Boss/BossClient.h>

#include <Poco/DateTime.h>
#include <Poco/DateTimeFormat.h>
#include <Poco/DateTimeParser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_CENTRALIZATION_LOCAL_LOCK_TIMEOUT;
}

HistoryCleanupTask::HistoryCleanupTask(
    ContextPtr context_,
    MetadataCentralizationManager * manager_,
    String cleanup_markers_path_,
    UInt32 cleanup_interval_seconds_,
    UInt32 cleanup_retention_days_)
    : WithContext(context_)
    , manager(manager_)
    , cleanup_markers_path(cleanup_markers_path_)
    , cleanup_interval_seconds(cleanup_interval_seconds_)
    , cleanup_retention_days(cleanup_retention_days_)
    , log(getLogger("HistoryCleanupTask"))
{
}

void HistoryCleanupTask::run()
{
    LOG_DEBUG(log, "Starting history cleanup task check");

    if (!manager)
    {
        LOG_ERROR(log, "Manager pointer is null, cannot run cleanup task");
        return;
    }

    /// Check Boss availability before attempting cleanup
    if (!manager->getBossAvailableFlag())
    {
        LOG_WARNING(log, "Boss service is unavailable, skipping history cleanup: {}", manager->getBossLastError());
        return;
    }

    try
    {
        /// Check if cleanup has already been done today
        if (isCleanupDoneToday())
        {
            LOG_DEBUG(log, "Cleanup has already been done today, skipping");
            return;
        }

        /// Execute cleanup with retry logic
        const std::vector<UInt32> retry_delays = {30, 60, 120}; // seconds

        for (size_t attempt = 0; attempt <= retry_delays.size(); ++attempt)
        {
            if (is_stopped.load())
            {
                LOG_DEBUG(log, "Task stopped during cleanup attempts");
                return;
            }

            try
            {
                LOG_INFO(log, "Attempting cleanup (attempt {}/{})", attempt + 1, retry_delays.size() + 1);

                /// Execute cleanup and only proceed if completely successful
                if (executeCleanup())
                {
                    LOG_INFO(log, "Cleanup completed successfully");

                    /// CRITICAL: Only mark as done if cleanup was completely successful
                    /// This ensures the ZooKeeper marker is only created when all files
                    /// were successfully deleted without any exceptions
                    markCleanupDone();
                    return;
                }
                else
                {
                    LOG_WARNING(log, "Cleanup attempt {} failed", attempt + 1);
                }
            }
            catch (...)
            {
                tryLogCurrentException(log, fmt::format("Cleanup attempt {} failed with exception", attempt + 1));
            }

            /// If not the last attempt, sleep before retrying
            if (attempt < retry_delays.size())
            {
                UInt32 sleep_seconds = retry_delays[attempt];
                LOG_INFO(log, "Sleeping {} seconds before retry", sleep_seconds);

                /// Sleep in 5-second intervals to allow for responsive shutdown
                for (UInt32 i = 0; i < sleep_seconds && !is_stopped.load(); i += 5)
                {
                    std::this_thread::sleep_for(std::chrono::seconds(5));
                }
            }
        }

        LOG_ERROR(log, "Cleanup failed after all retry attempts");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Unexpected error in history cleanup task");
    }
}

void HistoryCleanupTask::threadFunction()
{
    LOG_INFO(log, "History cleanup thread started with check interval {} seconds", cleanup_interval_seconds);

    while (!is_stopped.load())
    {
        try
        {
            /// Check if cleanup has been done today
            if (isCleanupDoneToday())
            {
                LOG_DEBUG(log, "Cleanup has already been done today, sleeping for 24 hours");

                /// Sleep for 24 hours, checking periodically if we should stop
                /// Break the sleep into 60-second intervals for responsive shutdown
                constexpr UInt32 one_day_seconds = 24 * 60 * 60;
                for (UInt32 i = 0; i < one_day_seconds && !is_stopped.load(); i += 60)
                {
                    std::this_thread::sleep_for(std::chrono::seconds(60));
                }
                continue;
            }

            /// Execute cleanup task
            run();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Unexpected error in history cleanup thread");
        }

        /// Sleep for the check interval
        /// Break the sleep into 5-second intervals for responsive shutdown
        for (UInt32 i = 0; i < cleanup_interval_seconds && !is_stopped.load(); i += 5)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    }

    LOG_INFO(log, "History cleanup thread stopped");
}

bool HistoryCleanupTask::isCleanupDoneToday()
{
    try
    {
        auto zookeeper = getContext()->getZooKeeper();
        if (!zookeeper)
        {
            LOG_WARNING(log, "ZooKeeper not available");
            return false;
        }

        Poco::DateTime now;
        String date_str = Poco::DateTimeFormatter::format(now, "%Y%m%d");

        String marker_path = fmt::format("{}/{}", cleanup_markers_path, date_str);

        if (zookeeper->exists(marker_path))
        {
            String marker_data = zookeeper->get(marker_path);
            LOG_DEBUG(log, "Found cleanup marker for today: {} with data: {}", marker_path, marker_data);
            return true;
        }

        LOG_DEBUG(log, "No cleanup marker found for today: {}", marker_path);
        return false;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to check if cleanup is done today");
        return false;
    }
}

void HistoryCleanupTask::markCleanupDone()
{
    try
    {
        auto zookeeper = getContext()->getZooKeeper();
        if (!zookeeper)
        {
            LOG_WARNING(log, "ZooKeeper not available, cannot mark cleanup as done");
            return;
        }

        Poco::DateTime now;
        String date_str = Poco::DateTimeFormatter::format(now, "%Y%m%d");
        String timestamp_str = Poco::DateTimeFormatter::format(now, "%Y%m%d_%H%M%S");

        String marker_base_path = manager->getConfig().cleanup_markers_path;
        String marker_path = fmt::format("{}/{}", marker_base_path, date_str);

        char hostname_buf[256];
        String node_name = "unknown";
        if (gethostname(hostname_buf, sizeof(hostname_buf)) == 0)
            node_name = hostname_buf;

        String marker_data = fmt::format("timestamp={},node={}", timestamp_str, node_name);

        zookeeper->createAncestors(marker_path);

        /// Try to create the marker node
        /// Use Persistent mode so it doesn't disappear when connection is lost
        /// Multiple nodes may try to create this at the same time, but only one will succeed
        /// This implements the competition mechanism - first node to create wins
        auto create_result = zookeeper->tryCreate(marker_path, marker_data, zkutil::CreateMode::Persistent);

        if (create_result == Coordination::Error::ZOK)
        {
            LOG_INFO(log, "Successfully marked cleanup as done for today: {} by node: {}", marker_path, node_name);

            /// Clean up old marker nodes (older than 180 days) to avoid accumulation
            cleanupOldMarkers(zookeeper, marker_base_path);
        }
        else if (create_result == Coordination::Error::ZNODEEXISTS)
        {
            String existing_data = zookeeper->get(marker_path);
            LOG_INFO(log, "Cleanup marker already exists for today: {} (created by: {})", marker_path, existing_data);
        }
        else
        {
            LOG_WARNING(log, "Failed to create cleanup marker: {}, error: {}", marker_path, create_result);
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to mark cleanup as done");
    }
}

void HistoryCleanupTask::cleanupOldMarkers(zkutil::ZooKeeperPtr zookeeper, const String & marker_base_path)
{
    try
    {
        if (!zookeeper->exists(marker_base_path))
            return;

        auto children = zookeeper->getChildren(marker_base_path);

        Poco::DateTime current_date;

        for (const auto & child : children)
        {
            try
            {
                if (child.length() != 8)
                    continue;

                int year = std::stoi(child.substr(0, 4));
                int month = std::stoi(child.substr(4, 2));
                int day = std::stoi(child.substr(6, 2));

                Poco::DateTime marker_date(year, month, day);

                Poco::Timespan diff = current_date - marker_date;
                int days_old = diff.days();

                /// Delete markers older than 180 days
                if (days_old > 180)
                {
                    String old_marker_path = fmt::format("{}/{}", marker_base_path, child);
                    zookeeper->tryRemove(old_marker_path);
                    LOG_DEBUG(log, "Removed old cleanup marker: {} (age: {} days)", old_marker_path, days_old);
                }
            }
            catch (...)
            {
                continue;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to cleanup old markers");
    }
}

bool HistoryCleanupTask::executeCleanup()
{
    LOG_INFO(log, "Executing history cleanup");

    try
    {
        String operation_name = "history cleanup";
        auto lock = manager->acquireDistLock(operation_name);

        LOG_DEBUG(log, "Acquired locks for cleanup operation");

        auto referenced_files = getReferencedVersionFiles();
        LOG_DEBUG(log, "Found {} referenced version files in manifest", referenced_files.size());

        auto boss_client = manager->getBossClientPtr();
        if (!boss_client)
        {
            LOG_ERROR(log, "Boss client not available");
            return false;
        }

        auto all_objects = boss_client->listObjectsWithMetadata();

        LOG_DEBUG(log, "Found {} total objects in Boss storage", all_objects.size());

        auto now = std::chrono::system_clock::now();
        auto cutoff_time = now - std::chrono::hours(24 * cleanup_retention_days);

        std::vector<String> files_to_delete;
        for (const auto & obj : all_objects)
        {
            const String & key = obj.key;

            if (!key.ends_with(".sql"))
                continue;

            if (referenced_files.contains(key))
            {
                LOG_DEBUG(log, "Skipping referenced file: {}", key);
                continue;
            }

            if (obj.last_modified < cutoff_time)
            {
                auto age = std::chrono::duration_cast<std::chrono::hours>(now - obj.last_modified).count() / 24;
                LOG_DEBUG(log, "File {} is {} days old, marked for deletion", key, age);
                files_to_delete.push_back(key);
            }
        }

        LOG_INFO(log, "Found {} files to delete (older than {} days and not referenced)",
                 files_to_delete.size(), cleanup_retention_days);

        if (!files_to_delete.empty())
        {
            size_t deleted_count = boss_client->deleteObjects(files_to_delete);

            if (deleted_count == files_to_delete.size())
            {
                LOG_INFO(log, "Successfully deleted all {}/{} files", deleted_count, files_to_delete.size());
                return true;
            }
            else
            {
                LOG_ERROR(log, "Failed to delete all files: only {}/{} files were deleted", deleted_count, files_to_delete.size());
                return false;
            }
        }
        else
        {
            LOG_INFO(log, "No files to delete, cleanup task completed successfully");
            return true;
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to execute cleanup");
        return false;
    }
}

std::unordered_set<String> HistoryCleanupTask::getReferencedVersionFiles()
{
    std::unordered_set<String> referenced_files;

    try
    {
        const auto & manifest_synchronizer = manager->getManifestSynchronizer();
        if (!manifest_synchronizer)
        {
            LOG_ERROR(log, "Manifest synchronizer not available");
            return referenced_files;
        }

        LOG_DEBUG(log, "Downloading latest manifest from Boss to get referenced files");
        ManifestPtr manifest = manifest_synchronizer->downloadFromRemote();

        if (!manifest)
        {
            LOG_ERROR(log, "Failed to download manifest from Boss");
            return referenced_files;
        }

        LOG_DEBUG(log, "Successfully downloaded manifest from Boss (etag: {}, databases: {})",
                  manifest->etag, manifest->databases.size());

        for (const auto & db : manifest->databases)
        {
            if (!db.key.empty())
            {
                referenced_files.insert(db.key);
                LOG_DEBUG(log, "Referenced database file: {}", db.key);
            }

            for (const auto & table : db.tables)
            {
                if (!table.key.empty())
                {
                    referenced_files.insert(table.key);
                    LOG_DEBUG(log, "Referenced table file: {}", table.key);
                }
            }
        }

        LOG_INFO(log, "Found {} total referenced files from latest manifest", referenced_files.size());
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to get referenced version files from Boss");
    }

    return referenced_files;
}

void HistoryCleanupTask::start()
{
    if (is_stopped.load())
        return;

    LOG_INFO(log, "Starting history cleanup task with check interval {} seconds", cleanup_interval_seconds);
    try
    {
        cleanup_thread = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });
    }
    catch (...)
    {
        is_stopped.store(true);
        tryLogCurrentException(log, "Failed to start history cleanup task");
        throw;
    }
}

void HistoryCleanupTask::shutdown()
{
    if (is_stopped.exchange(true))
        return;

    LOG_INFO(log, "Shutting down history cleanup task");
    try
    {
        /// Wait for the thread to finish
        if (cleanup_thread.joinable())
            cleanup_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error during history cleanup task shutdown");
    }
}

HistoryCleanupTask::~HistoryCleanupTask()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in HistoryCleanupTask destructor");
    }
}

}
