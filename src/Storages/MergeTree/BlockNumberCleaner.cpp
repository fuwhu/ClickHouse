#include "BlockNumberCleaner.h"

#include <chrono>
#include <filesystem>
#include <mutex>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Databases/IDatabase.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/setThreadName.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

std::unique_ptr<BlockNumberCleaner> BlockNumberCleaner::block_number_cleaner;

void BlockNumberCleaner::init(ContextPtr context_)
{
    if (block_number_cleaner)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block number cleaner is initialized twice. This is a bug.");

    block_number_cleaner.reset(new BlockNumberCleaner(context_));
}

BlockNumberCleaner & BlockNumberCleaner::instance()
{
    if (!block_number_cleaner)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Block number cleaner is not initialized. This is a bug.");

    return *block_number_cleaner;
}

BlockNumberCleaner::BlockNumberCleaner(ContextPtr context_) : WithContext(context_), log(getLogger("BlockNumberCleaner"))
{
    cleanup_batch_interval = context_->getSettingsRef().block_number_cleanup_batch_interval;
    cleanup_batch_size = context_->getSettingsRef().block_number_cleanup_batch_size;
    cleanup_execution_interval = context_->getSettingsRef().block_number_cleanup_execution_interval;
    cleanup_timediff_hours = context_->getSettingsRef().block_number_cleanup_timediff_hours;
}

BlockNumberCleaner::~BlockNumberCleaner()
{
    stopBackgroundThread();
}

void BlockNumberCleaner::startup()
{
    std::lock_guard lock{mutex};
    can_be_cleaned = true;

    if (!thread.joinable())
        thread = ThreadFromGlobalPool{&BlockNumberCleaner::backGroundCleanFunc, this};
}

void BlockNumberCleaner::shutdown()
{
    block_number_cleaner.reset();
}

void BlockNumberCleaner::backGroundCleanFunc()
{
    setThreadName("BlockNumClean");

    std::unique_lock lock{mutex};

    while (can_be_cleaned)
    {
        std::chrono::system_clock::time_point current_time = std::chrono::system_clock::now();
        try
        {
            LOG_DEBUG(log, "Begin to search and clean");

            DB::Databases databases = DatabaseCatalog::instance().getDatabases();
            int to_sleep_number = 0;

            for (const auto & database : databases)
            {
                if (DatabaseCatalog::isPredefinedDatabase(database.first))
                    continue;

                if (!database.second->canContainMergeTreeTables())
                    continue;

                for (auto it = database.second->getTablesIterator(getContext()); it->isValid(); it->next())
                {
                    StoragePtr table = it->table();
                    if (!table)
                        continue;

                    StorageReplicatedMergeTree * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
                    if (replicated_table && replicated_table->getInMemoryMetadataPtr()->hasPartitionKey()
                        && !replicated_table->isTableReadOnly() && replicated_table->is_leader)
                    {
                        UInt64 total_cleanup_size = checkAndCleanZnodes(replicated_table);
                        if (total_cleanup_size > 0)
                        {
                            StorageID table_id = replicated_table->getStorageID();
                            LOG_INFO(
                                log,
                                "Finish cleaning {} block numbers of {}.{}",
                                total_cleanup_size,
                                table_id.getDatabaseName(),
                                table_id.getTableName());
                        }

                        to_sleep_number++;
                        to_sleep_number %= cleanup_batch_size;

                        if (!to_sleep_number)
                        {
                            clean_condition.wait_for(lock, cleanup_batch_interval, [&]() -> bool { return !can_be_cleaned; });
                            if (!can_be_cleaned)
                                return;
                        }
                    }
                }
            }

            clean_condition.wait_until(lock, current_time + cleanup_execution_interval, [&]() -> bool { return !can_be_cleaned; });
        }
        catch (...)
        {
            tryLogCurrentException(log, "While cleaning the nodes it has error");
        }
    }
}

UInt64 BlockNumberCleaner::checkAndCleanZnodes(StorageReplicatedMergeTree * replicated_table)
{
    if (!can_be_cleaned)
        return 0;

    UInt64 total_cleanup_size = 0;

    auto zk = replicated_table->getZooKeeperAndAssertNotReadonly();

    auto block_numbers_path = fs::path(replicated_table->zookeeper_path) / "block_numbers";
    zkutil::Strings partitions = zk->getChildren(block_numbers_path);

    Coordination::Requests ops;

    auto do_clean_znodes = [&]()
    {
        Coordination::Responses results;
        Coordination::Error code = zk->tryMulti(ops, results);

        if (code != Coordination::Error::ZOK)
        {
            for (size_t i = 0; i < results.size(); ++i)
            {
                if (results[i]->error != Coordination::Error::ZOK)
                    LOG_WARNING(
                        log, "Error while delete zookeeper path {} : {}", ops[i]->getPath(), Coordination::errorMessage(results[i]->error));
                else
                {
                    total_cleanup_size += 1;
                    replicated_table->erasePathCache(ops[i]->getPath());
                }
            }
        }
        else
        {
            total_cleanup_size += results.size();
            for (size_t i = 0; i < results.size(); ++i)
                replicated_table->erasePathCache(ops[i]->getPath());
        }
    };

    for (const auto & partition : partitions)
    {
        /// delete the znode that is created one month ago
        Coordination::Stat stat;
        zk->get(fs::path(block_numbers_path) / partition, &stat);

        auto max_cleanup_timestamp = std::chrono::duration_cast<std::chrono::milliseconds>(
                                         (std::chrono::system_clock::now() - std::chrono::hours(cleanup_timediff_hours)).time_since_epoch())
                                         .count();

        if (stat.ctime > max_cleanup_timestamp)
            continue;

        auto data_parts = replicated_table->getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, partition);
        if (data_parts.empty())
            ops.emplace_back(zkutil::makeRemoveRequest(block_numbers_path / partition, -1));

        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE)
        {
            do_clean_znodes();
            ops.clear();
        }
    }

    if (!ops.empty())
        do_clean_znodes();

    return total_cleanup_size;
}

void BlockNumberCleaner::stopBackgroundThread()
{
    can_be_cleaned = false;
    clean_condition.notify_all();

    if (thread.joinable())
        thread.join();
}
}
