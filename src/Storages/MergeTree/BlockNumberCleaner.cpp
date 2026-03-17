#include <algorithm>
#include <chrono>
#include <filesystem>
#include <mutex>
#include <thread>
#include <unistd.h>
#include <Databases/IDatabase.h>
#include <Storages/MergeTree/BlockNumberCleaner.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Poco/Logger.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/setThreadName.h>
#include <Common/Exception.h>
#include <Common/ZooKeeper/Common.h>
#include <Common/ZooKeeper/IKeeper.h>
#include <Common/ZooKeeper/Types.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <base/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::unique_ptr<BlockNumberCleaner> BlockNumberCleaner::singleton_instance;

BlockNumberCleaner::BlockNumberCleaner(ContextMutablePtr context_, std::chrono::seconds interval_seconds_) : WithMutableContext(context_), log(&Poco::Logger::get("BlockNumberCleaner"))
{
    interval_seconds = interval_seconds_;
}

void BlockNumberCleaner::init(ContextMutablePtr context_, std::chrono::seconds interval_seconds_)
{   
    if (singleton_instance)
    {
        throw Exception("The singleton instance has already been initied", ErrorCodes::LOGICAL_ERROR);
    }
    singleton_instance.reset(new BlockNumberCleaner(context_, interval_seconds_));
}

void BlockNumberCleaner::startup()
{
    can_be_cleaned = true;

    std::lock_guard lock{mutex};
    if (!thread.joinable())
        thread = ThreadFromGlobalPool{&BlockNumberCleaner::backGroundCleanFunc, this};
} 

void BlockNumberCleaner::shutdown()
{
    singleton_instance.reset();
}

BlockNumberCleaner::~BlockNumberCleaner()
{
    stopBackgroundThread();
}

void BlockNumberCleaner::backGroundCleanFunc()
{
    setThreadName("CleanBlockNumbers");

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
                for (auto it = database.second->getTablesIterator(getContext()); it->isValid(); it->next())
                {
                    StoragePtr table = it->table();
                    if (!table || it->databaseName() == "system")
                        continue;

                    StorageReplicatedMergeTree * replicated_table = dynamic_cast<StorageReplicatedMergeTree *>(table.get());
                    try
                    {
                        if (replicated_table && replicated_table->is_leader)
                        {
                            checkAndCleanZnodes(replicated_table);
                            to_sleep_number++;
                            to_sleep_number %= getContext()->getSettingsRef().block_number_cleanup_batch_size;

                            if (!to_sleep_number)
                                sleep(getContext()->getSettingsRef().block_number_cleanup_batch_interval.value.seconds());
                        }
                    }
                    catch (const Coordination::Exception & e)
                    {
                        if (Coordination::isHardwareError(e.code))
                            throw;
                        auto storage_id = replicated_table->getStorageID();
                        tryLogCurrentException(log, "While cleaning the nodes of " + storage_id.getDatabaseName() + "." + storage_id.getTableName() + " has error ");
                    }
                    catch (...)
                    {
                        auto storage_id = replicated_table->getStorageID();
                        tryLogCurrentException(log, "While cleaning the nodes of " + storage_id.getDatabaseName() + "." + storage_id.getTableName() + " has error ");   
                    }
                }
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "While cleaning the nodes it has error ");
        }
        

        clean_condition.wait_until(lock, current_time + interval_seconds);
    }
}

void BlockNumberCleaner::checkAndCleanZnodes(StorageReplicatedMergeTree * replicated_table)
{
    auto zk = replicated_table->getZooKeeperAndAssertNotReadonly();

    auto block_numbers_path = fs::path(replicated_table->zookeeper_path) / "block_numbers";
    zkutil::Strings partitions = zk->getChildren(block_numbers_path);

    if (partitions.empty() || std::find(partitions.begin(), partitions.end(), "all") != partitions.end())  return;

    Coordination::Requests ops;

    auto do_clean_znodes = [&]() {
        Coordination::Responses results;
        Coordination::Error code = zk->tryMulti(ops, results);

        if (code != Coordination::Error::ZOK)
        {
            for (size_t i = 0; i < results.size(); ++i)
            {
                if (results[i]->error != Coordination::Error::ZOK)
                    LOG_WARNING(log, "Error while delete zookeeper path {} : {}", ops[i]->getPath(), Coordination::errorMessage(results[i]->error));
                else
                    replicated_table->erasePathCache(ops[i]->getPath());
            }
        }
        else
        {
            for (size_t i = 0; i < results.size(); ++i)
                replicated_table->erasePathCache(ops[i]->getPath());
        }
    };

    /// Collect partition IDs that currently have entries in the replication queue.
    /// These partitions may be in the middle of a sync (GET_PART, MERGE_PARTS, etc.),
    /// so we must not delete their block_numbers znodes.
    std::unordered_set<String> partitions_in_queue;
    {
        ReplicatedMergeTreeQueue::LogEntriesData entries;
        replicated_table->queue.getEntries(entries);
        for (const auto & entry : entries)
        {
            if (!entry.new_part_name.empty())
            {
                auto part_info = MergeTreePartInfo::tryParsePartName(entry.new_part_name, replicated_table->format_version);
                if (part_info)
                    partitions_in_queue.insert(part_info->partition_id);
            }
        }
    }

    for (auto it = partitions.begin(); it != partitions.end(); it++)
    {
        /// Skip partitions that have pending entries in the replication queue to avoid
        /// deleting block_numbers znodes that are still needed for in-progress syncs.
        if (partitions_in_queue.contains(*it))
            continue;

        auto data_parts = replicated_table->getDataPartsVectorInPartition(MergeTreeDataPartState::Active, *it);
        
        /// delete the znode that is created one month ago
        Coordination::Stat stat;
        zk->get(fs::path(block_numbers_path) / *it, &stat);

        auto one_month_ago = std::chrono::system_clock::now() - std::chrono::hours(getContext()->getSettingsRef().block_number_cleanup_timediff_hours);
        auto one_month_ago_time_stamp = std::chrono::duration_cast<std::chrono::milliseconds>(one_month_ago.time_since_epoch()).count();

        if (data_parts.empty() && stat.ctime <= one_month_ago_time_stamp)
        {
            ops.emplace_back(zkutil::makeRemoveRequest(block_numbers_path / *it, -1));
        }
        else 
            continue;
        
        if (ops.size() > 4 * zkutil::MULTI_BATCH_SIZE)
        {
            do_clean_znodes();
            ops.clear();
        }
    }

    if (!ops.empty())
        do_clean_znodes();
    
    StorageID table = replicated_table->getStorageID();

    LOG_DEBUG(log, "Finishing cleaning the {}.{}", table.getDatabaseName(), table.getTableName());

}

void BlockNumberCleaner::stopBackgroundThread()
{
    can_be_cleaned = false;
    clean_condition.notify_one();

    if (thread.joinable())
        thread.join();
}


}
