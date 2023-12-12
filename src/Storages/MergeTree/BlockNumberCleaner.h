#pragma once

#include <Common/ThreadPool.h>
#include <Interpreters/Context_fwd.h>
#include <Poco/Logger.h>
#include <chrono>
#include <condition_variable>


namespace DB 
{

class StorageReplicatedMergeTree;

class BlockNumberCleaner : public WithMutableContext
{
public:
    static BlockNumberCleaner & instance() { return *singleton_instance; }

    static void init(ContextMutablePtr context_, std::chrono::seconds interval_seconds_);

    void startup();

    static void shutdown();

private:
    friend std::unique_ptr<BlockNumberCleaner>::deleter_type;

    explicit BlockNumberCleaner(ContextMutablePtr context_, std::chrono::seconds interval_seconds_);
    ~BlockNumberCleaner();

    void backGroundCleanFunc();
    void stopBackgroundThread();

    void getTablePartitions();

    void checkAndCleanZnodes(StorageReplicatedMergeTree * replicated_table);

    static std::unique_ptr<BlockNumberCleaner> singleton_instance;

    std::mutex mutex;
    std::condition_variable clean_condition;

    Poco::Logger * log;

    std::chrono::seconds interval_seconds;
    std::atomic<bool> can_be_cleaned = false;
    ThreadFromGlobalPool thread;
};

}
