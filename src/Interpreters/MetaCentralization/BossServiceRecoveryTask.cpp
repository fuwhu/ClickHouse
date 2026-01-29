#include <Interpreters/MetaCentralization/BossServiceRecoveryTask.h>

#include <algorithm>
#include <chrono>

#include <Interpreters/Context.h>
#include <Interpreters/MetaCentralization/MetadataCentralizationManager.h>
#include <IO/Boss/BossClient.h>

namespace DB
{

BossServiceRecoveryTask::BossServiceRecoveryTask(
    ContextPtr context_,
    MetadataCentralizationManager * manager_,
    UInt32 recovery_interval_seconds_)
    : WithContext(context_)
    , manager(manager_)
    , recovery_interval_seconds(recovery_interval_seconds_)
    , log(getLogger("BossServiceRecoveryTask"))
{
}

void BossServiceRecoveryTask::run()
{
    LOG_DEBUG(log, "Checking Boss service availability");

    try
    {
        bool boss_is_available = tryStartup();

        if (boss_is_available)
            consecutive_check_failures = 0;
        else
            consecutive_check_failures++;
    }
    catch (...)
    {
        consecutive_check_failures++;
        tryLogCurrentException(log, "Failed to recover Boss service. Will try again");
    }
}

void BossServiceRecoveryTask::threadFunction()
{
    LOG_INFO(log, "Boss service recovery thread started with check interval {} seconds", recovery_interval_seconds);

    while (!need_stop.load())
    {
        try
        {
            run();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Unexpected error in Boss service recovery thread");
        }

        /// Calculate next sleep duration using exponential backoff with random jitter
        size_t next_retry_ms = recovery_interval_seconds * 1000;

        if (consecutive_check_failures > 0) 
        {
            size_t exp_factor;
            if (consecutive_check_failures <= 7) {
                exp_factor = 1 << (consecutive_check_failures - 1);
            } else {
                exp_factor = 128;  // 1 << 7
            }
            size_t current_base = BASE_DELAY_MS * exp_factor;
            
            current_base = std::min(current_base, MAX_DELAY_MS);
            
            size_t min_delay;
            size_t max_delay;
            
            if (consecutive_check_failures == 1) {
                min_delay = max_delay = BASE_DELAY_MS;
            } else {
                min_delay = current_base / 2;
                max_delay = current_base;
            }
            
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(min_delay, max_delay);
            
            next_retry_ms = dis(gen);
        }

        /// Wait with condition variable for wakeup support
        std::unique_lock<std::mutex> lock(wakeup_mutex);
        wakeup_cv.wait_for(lock, std::chrono::milliseconds(next_retry_ms), [this]() {
            return need_stop.load() || wakeup_flag.load();
        });

        /// Reset wakeup flag
        wakeup_flag.store(false);
    }

    LOG_INFO(log, "Boss service recovery thread stopped");
}

bool BossServiceRecoveryTask::tryStartup()
{
    if (!manager)
    {
        LOG_ERROR(log, "Manager pointer is null");
        return false;
    }

    if (manager->getBossAvailableFlag())
    {
        LOG_INFO(log, "Boss service is already available");
        return true;
    }

    LOG_WARNING(log, "Boss service is unavailable. Attempting recovery (consecutive failures: {})", consecutive_check_failures);

    try
    {
        /// Threshold for forcing BossClient recreation
        const UInt32 force_recreate_threshold = 5;

        /// Check if BossClient is initialized, reinitialize if needed
        auto boss_client = manager->getBossClientPtr();
        bool need_reinit = !boss_client;
        bool force_recreate = false;

        /// If client exists but we have consecutive failures, force recreate
        if (boss_client && consecutive_check_failures >= force_recreate_threshold)
        {
            LOG_WARNING(log, "BossClient exists but {} consecutive failures detected, forcing recreation", consecutive_check_failures);
            need_reinit = true;
            force_recreate = true;
        }

        if (need_reinit)
        {
            LOG_INFO(log, "BossClient is {}, attempting to {}initialize",
                     boss_client ? "stale" : "null",
                     force_recreate ? "force re" : "re");

            if (!manager->reinitializeBossClient(force_recreate))
            {
                LOG_ERROR(log, "Failed to reinitialize BossClient");
                return false;
            }
            LOG_INFO(log, "BossClient reinitialized successfully");
        }

        /// Check Boss service status
        bool is_service_active = false;
        bool has_manifest = false;
        manager->probeBossService(is_service_active, has_manifest);

        if (!is_service_active)
        {
            LOG_WARNING(log, "Boss service is not responding");
            return false;
        }

        /// If manifest doesn't exist, initialize it
        if (!has_manifest)
        {
            LOG_ERROR(log, "No manifest found in Boss, Setting Boss service as unavailable.");
            manager->setBossUnavailable("No Boss manifest exists");
            return false;
        }

        LOG_INFO(log, "Boss service recovered and is now available");
        manager->setBossAvailable();
        return true;
    }
    catch (...)
    {
        tryLogCurrentException(log, "Boss service remains unavailable");
        return false;
    }
}

void BossServiceRecoveryTask::start()
{
    if (need_stop.load())
        return;

    LOG_INFO(log, "Starting Boss service recovery task with check interval {} seconds", recovery_interval_seconds);
    try
    {
        recovery_thread = ThreadFromGlobalPoolNoTracingContextPropagation([this] { threadFunction(); });
    }
    catch (...)
    {
        tryLogCurrentException(log, "Failed to start Boss service recovery task");
        throw;
    }
}

void BossServiceRecoveryTask::wakeup()
{
    if (need_stop.load())
        return;

    LOG_DEBUG(log, "Waking up Boss service recovery task");

    /// Set wakeup flag and notify the condition variable
    {
        std::lock_guard<std::mutex> lock(wakeup_mutex);
        wakeup_flag.store(true);
    }
    wakeup_cv.notify_one();
}

void BossServiceRecoveryTask::shutdown()
{
    if (need_stop.exchange(true))
        return;

    LOG_INFO(log, "Shutting down Boss service recovery task");

    /// Notify the thread to wake up and exit
    {
        std::lock_guard<std::mutex> lock(wakeup_mutex);
        wakeup_flag.store(true);
    }
    wakeup_cv.notify_one();

    try
    {
        /// Wait for the thread to finish
        if (recovery_thread.joinable())
            recovery_thread.join();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error during Boss service recovery task shutdown");
    }
}

BossServiceRecoveryTask::~BossServiceRecoveryTask()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(log, "Error in BossServiceRecoveryTask destructor");
    }
}

}
