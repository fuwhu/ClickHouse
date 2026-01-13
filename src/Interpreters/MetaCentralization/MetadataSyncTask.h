#pragma once

#include <atomic>
#include <memory>

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MetadataCentralizationManager;

/// Background task for periodic metadata synchronization
class MetadataSyncTask : public WithContext
{
public:
    MetadataSyncTask(
        ContextPtr context_,
        MetadataCentralizationManager * manager_,
        UInt32 sync_interval_seconds_);

    ~MetadataSyncTask();

    void start();

    void shutdown();

private:
    void run();

    void threadFunction();

    ThreadFromGlobalPoolNoTracingContextPropagation sync_thread;

    MetadataCentralizationManager * manager;
    UInt32 sync_interval_seconds;

    std::atomic<bool> is_stopped{false};
    LoggerPtr log;
};

using MetadataSyncTaskPtr = std::unique_ptr<MetadataSyncTask>;
}
