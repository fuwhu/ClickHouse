#include <Interpreters/MetaCentralization/MetadataSyncTask.h>

#include <chrono>
#include <thread>

#include <Common/Exception.h>
#include <Interpreters/Context.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int METADATA_CENTRALIZATION_LOCAL_LOCK_TIMEOUT;
}

MetadataSyncTask::MetadataSyncTask(
    ContextPtr context_,
    MetadataCentralizationManager * manager_,
    UInt32 sync_interval_seconds_)
    : WithContext(context_)
    , manager(manager_)
    , sync_interval_seconds(sync_interval_seconds_)
    , log(getLogger("MetadataSyncTask"))
{
}

void MetadataSyncTask::run()
{
    LOG_DEBUG(log, "Starting metadata synchronization task");

    if (!manager)
    {
        LOG_ERROR(log, "Manager pointer is null, cannot run sync task");
        return;
    }

    /// Check Boss availability before attempting sync
    if (!manager->getBossAvailableFlag())
    {
        LOG_WARNING(log, "Boss service is unavailable, skipping metadata synchronization: {}", manager->getBossLastError());
        return;
    }

    try
    {
        manager->syncMetadataFromBoss();
        LOG_DEBUG(log, "Metadata synchronization completed successfully");
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to synchronize metadata from Boss");
    }
}

void MetadataSyncTask::threadFunction()
{
    LOG_INFO(log, "Metadata sync thread started with interval {} seconds", sync_interval_seconds);

    while (!is_stopped.load())
    {
        try
        {
            run();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Unexpected error in metadata sync thread");
        }

        /// Sleep for the configured interval, checking periodically if we should stop
        /// Break the sleep into 5-second intervals to allow for responsive shutdown
        for (UInt32 i = 0; i < sync_interval_seconds && !is_stopped.load(); i += 5)
        {
            std::this_thread::sleep_for(std::chrono::seconds(5));
        }
    }

    LOG_INFO(log, "Metadata sync thread stopped");
}

void MetadataSyncTask::start()
{
    if (is_stopped.load())
        return;

    LOG_INFO(log, "Starting metadata sync task with interval {} seconds", sync_interval_seconds);
    try
    {
        sync_thread = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });
    }
    catch (...)
    {
        is_stopped.store(true);
        tryLogCurrentException(log, "Failed to start metadata sync task");
        throw;
    }
}

void MetadataSyncTask::shutdown()
{
    if (is_stopped.exchange(true))
        return;

    LOG_INFO(log, "Shutting down metadata sync task");
    try
    {
        /// Wait for the thread to finish
        if (sync_thread.joinable())
            sync_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error during metadata sync task shutdown");
    }
}

MetadataSyncTask::~MetadataSyncTask()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in MetadataSyncTask destructor");
    }
}

}
