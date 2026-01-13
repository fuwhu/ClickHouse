#pragma once

#include <atomic>
#include <memory>
#include <condition_variable>
#include <mutex>

#include <base/types.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Interpreters/Context_fwd.h>

namespace DB
{

class MetadataCentralizationManager;
class BossClient;
using BossClientPtr = std::shared_ptr<BossClient>;

/// Background task for Boss service recovery
/// Monitors Boss availability and attempts recovery when service is unavailable
class BossServiceRecoveryTask : public WithContext
{
public:
    BossServiceRecoveryTask(
        ContextPtr context_,
        MetadataCentralizationManager * manager_,
        UInt32 recovery_interval_seconds_);

    ~BossServiceRecoveryTask();

    void start();

    /// Wake up the recovery task to check Boss service immediately
    void wakeup();

    void shutdown();

private:
    void run();

    void threadFunction();

    /// Try to recover Boss service connection and initialize client if needed
    /// Returns true if Boss is available, false otherwise
    bool tryStartup();

    ThreadFromGlobalPoolNoTracingContextPropagation recovery_thread;

    MetadataCentralizationManager * manager;
    UInt32 recovery_interval_seconds;

    /// Track consecutive failures for backoff calculation
    UInt32 consecutive_check_failures = 0;

    std::atomic<bool> need_stop{false};

    /// For wakeup support
    std::mutex wakeup_mutex;
    std::condition_variable wakeup_cv;
    std::atomic<bool> wakeup_flag{false};

    LoggerPtr log;

    static constexpr size_t BASE_DELAY_MS = 100;
    static constexpr size_t MAX_DELAY_MS = 10000;
};

using BossServiceRecoveryTaskPtr = std::unique_ptr<BossServiceRecoveryTask>;
}
