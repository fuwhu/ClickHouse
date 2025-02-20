#pragma once

#include <chrono>
#include <condition_variable>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <Common/ThreadPool.h>


namespace DB
{

class StorageReplicatedMergeTree;

class BlockNumberCleaner : public WithContext
{
public:
    static BlockNumberCleaner & instance();

    static void init(ContextPtr context_);

    void startup();

    static void shutdown();

    ~BlockNumberCleaner();

private:
    explicit BlockNumberCleaner(ContextPtr context_);

    void backGroundCleanFunc();

    void stopBackgroundThread();

    void getTablePartitions();

    UInt64 checkAndCleanZnodes(StorageReplicatedMergeTree * replicated_table);

    static std::unique_ptr<BlockNumberCleaner> block_number_cleaner;

    std::mutex mutex;
    std::condition_variable clean_condition;

    Poco::LoggerPtr log;

    std::chrono::seconds cleanup_batch_interval;
    UInt64 cleanup_batch_size;
    std::chrono::seconds cleanup_execution_interval;
    UInt64 cleanup_timediff_hours;

    std::atomic<bool> can_be_cleaned = false;

    ThreadFromGlobalPool thread;
};

}
