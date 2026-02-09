#include "UniqueEngineDataWriter.h"

#include <utility>
#include <vector>
#include <Core/Settings.h>
// #include <Common/ErrorCodes.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/UniqueMergeTreeIndex.h>
#include <Storages/MergeTree/UniqueMergeTreeIndexCommon.h>

namespace CurrentMetrics
{
extern const Metric UniqueKeyUpdateThreads;
extern const Metric UniqueKeyUpdateThreadsActive;
extern const Metric UniqueKeyUpdateThreadsScheduled;
extern const Metric UniqueKeyBucketLoadThreads;
extern const Metric UniqueKeyBucketLoadThreadsActive;
extern const Metric UniqueKeyBucketLoadThreadsScheduled;
}

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 background_unique_engine_update_pool_size;
    extern const SettingsUInt64 background_unique_engine_update_schedule_timeout;
}
namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 unique_key_index_type;
    extern const MergeTreeSettingsUInt64 unique_key_update_parallelism;
    extern const MergeTreeSettingsUInt64 enable_unique_key_bucket;
    extern const MergeTreeSettingsUInt64 unique_key_bucket_load_parallelism;
    extern const MergeTreeSettingsBool unique_key_index_resident_in_memory;
    extern const MergeTreeSettingsUInt64 unique_delete_bitmap_type;
    extern const MergeTreeSettingsUInt64 unique_key_deduplicate_level;
    extern const MergeTreeSettingsUInt64 unique_key_update_parallel_type;
}

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

UniqueEngineDataWriter::UniqueEngineDataWriter(const MutableDataPartPtr & data_part_)
    : part_to_write(data_part_)
    , storage(data_part_->storage)
    , storage_settings(storage.getSettings())
    , settings(storage.getContext()->getSettingsRef())
    , log(getLogger(storage.getLogName()))
{
    const auto & unique_key_index_type = (*storage_settings)[MergeTreeSetting::unique_key_index_type];

    if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
    {
        update_thread_pool = std::make_shared<ThreadPool>(
            CurrentMetrics::UniqueKeyUpdateThreads,
            CurrentMetrics::UniqueKeyUpdateThreadsActive,
            CurrentMetrics::UniqueKeyUpdateThreadsScheduled,
            (*storage_settings)[MergeTreeSetting::unique_key_update_parallelism]);

        if ((*storage_settings)[MergeTreeSetting::enable_unique_key_bucket] && data_part_->commit_type != IMergeTreeDataPart::CommitType::EXECUTE_MERGE)
            loading_bucket_pool = std::make_shared<ThreadPool>(
                CurrentMetrics::UniqueKeyBucketLoadThreads,
                CurrentMetrics::UniqueKeyBucketLoadThreadsActive,
                CurrentMetrics::UniqueKeyBucketLoadThreadsScheduled,
                (*storage_settings)[MergeTreeSetting::unique_key_bucket_load_parallelism]);
    }
}

UniqueEngineDataWriter::~UniqueEngineDataWriter()
{
    if (has_temp_dir)
        clearTempDirs();
}

void UniqueEngineDataWriter::prepare()
{
    if (part_to_write->effective_rows_count)
    {
        if (part_to_write->commit_type == IMergeTreeDataPart::CommitType::NORMAL_INSERT)
            prepareForNewPart();
        else if (part_to_write->commit_type == IMergeTreeDataPart::CommitType::EXECUTE_MERGE)
            prepareForMergeOrMoveResultPart();
        else if (part_to_write->commit_type == IMergeTreeDataPart::CommitType::MERGE_BY_FETCH)
            prepareForMergeByFetchPart();
        else
            prepareForMergeOrMoveResultPart();
    }

    part_to_write->merge_source_parts.clear();
    part_to_write->move_source_part = nullptr;

    if (!(*storage_settings)[MergeTreeSetting::unique_key_index_resident_in_memory])
        part_to_write->clearUniqueKeyIndex();
}

void UniqueEngineDataWriter::dedupFunctionByPart(
    const ActiveDataPartPtrs & data_parts,
    const UniqueKeyIndexPtr & current_key_index,
    const UniqueDeleteBitmapPtr & current_delete_bitmap,
    size_t begin,
    size_t end)
{
    std::map<String, VersionAndRow> to_update_current;
    std::map<MutableDataPartPtr, std::vector<size_t>> to_update_normal;
    std::map<MutableDataPartPtr, DeletedKeysPtr> to_update_merging_moving;

    LOG_DEBUG(log, "[UniqueEngineDataWriter] current thread processes data part [{}, {}).", begin, end);

    /// Collect the data parts and corresponding data to update into to_update_current, to_update_normal and to_update_merging_moving.
    for (auto index = begin; index < end; ++index)
    {
        const auto & active_part = data_parts[index];
        if (active_part->effective_rows_count == 0)
        {
            LOG_DEBUG(log, "part {} effective_rows_count equal to zero, will skip directly.", active_part->name);
            continue;
        }

        /// pre check unique key minmax
        if (part_to_write->getUniqueKeyMinMaxIndex()->getMin() > active_part->getUniqueKeyMinMaxIndex()->getMax()
            || part_to_write->getUniqueKeyMinMaxIndex()->getMax() < active_part->getUniqueKeyMinMaxIndex()->getMin())
        {
            LOG_DEBUG(
                log,
                "part_to_write {} compare with active_part {}, min_idx greater than max_idx "
                "or max_idx less than min_idx, will skip directly.",
                part_to_write->name,
                active_part->name);
            continue;
        }

        /// pre confirm loading bucket range.
        BucketIndexRangePtr bucket_range;
        if (!(*storage_settings)[MergeTreeSetting::unique_key_index_resident_in_memory] && (*storage_settings)[MergeTreeSetting::enable_unique_key_bucket])
        {
            const auto & bucket_index = active_part->getUniqueKeyBucketIndex();
            if (bucket_index->getBucketNum() > 1)
            {
                const auto & target_bucket_range = current_key_index->calculateTargetBuckets(bucket_index->getBucketNum());

                if (target_bucket_range.empty())
                {
                    LOG_DEBUG(log, "part {} bucket_index_range is empty, will skip directly.", active_part->name);
                    continue;
                }

                bucket_range = std::make_shared<std::vector<size_t>>(target_bucket_range);
            }
        }

        const auto & delete_bitmap = active_part->getUniqueDeleteBitmap();
        current_key_index->forEach(
            [&](const StringRef & key, const VersionAndRow & mapped)
            {
                const String & current_key = key.toString();
                const UInt64 & current_version = std::get<0>(mapped);
                const size_t & current_rowid = std::get<1>(mapped);

                /// if new part is fetched from another replica, current row maybe already deleted by another replica, because delete bitmap is realtime updating.
                if (current_delete_bitmap->isDeleted(current_rowid))
                    return;

                auto active_key_index = active_part->getUniqueKeyIndex(true, loading_bucket_pool, bucket_range);
                const auto & active_version_rowid = active_key_index->get(current_key);
                if (active_version_rowid)
                {
                    size_t active_rowid = std::get<1>(active_version_rowid.value());
                    if (delete_bitmap->isDeleted(active_rowid))
                        return;

                    UInt64 active_version = std::get<0>(active_version_rowid.value());

                    compareWithActivePart(
                        to_update_current,
                        to_update_normal,
                        to_update_merging_moving,
                        current_key,
                        current_version,
                        current_rowid,
                        active_part,
                        active_version,
                        active_rowid);
                }
            });

        if (!(*storage_settings)[MergeTreeSetting::unique_key_index_resident_in_memory])
            active_part->clearUniqueKeyIndex();
    }

    /// Delete the duplicate rows in part_to_write and cache the updated data in delete_bitmap_map and key_indicies_map.
    /// Please note that the deleted rows still exist in part_to_write, they are only truly deleted after commit() finished.
    if (!to_update_current.empty())
    {
        enrollDataPart(part_to_write);
        PartToWriteLock part_to_write_lock = lockPartToWrite();
        for (auto & key_version_row : to_update_current)
        {
            const auto & pw_delete_bitmap = getDeleteBitmap(part_to_write);
            pw_delete_bitmap->deleteRow(std::get<1>(key_version_row.second));
        }
    }

    /// Delete the duplicate rows in every active data part and cache the updated data in delete_bitmap_map and key_indicies_map.
    /// Also please note that the deleted rows still exist in corresponding active data part, they are only truly deleted after commit() finished.
    for (const auto & pair : to_update_normal)
    {
        enrollDataPart(pair.first);
        for (const auto & row_to_delete : pair.second)
        {
            const auto & normal_delete_bitmap = getDeleteBitmap(pair.first);
            normal_delete_bitmap->deleteRow(row_to_delete);
        }
    }

    /// Flush all the deleted keys of the data parts being merged to corresponding temporary files.
    for (const auto & pair : to_update_merging_moving)
        enrollDataPart(pair.first, pair.second);
}

void UniqueEngineDataWriter::scheduleDedupTask(
    UniqueEngineDataWriter * uniq_engine_writer,
    const ActiveDataPartPtrs & data_parts,
    const UniqueKeyIndexPtr & current_key_index,
    const UniqueDeleteBitmapPtr & current_delete_bitmap,
    size_t begin,
    size_t end)
{
    auto max_running_update_task = uniq_engine_writer->settings[Setting::background_unique_engine_update_pool_size];
    size_t timeout_in_sec = uniq_engine_writer->settings[Setting::background_unique_engine_update_schedule_timeout];
    size_t waited_secs = 0;
    while (waited_secs <= timeout_in_sec)
    {
        size_t busy_threads_in_pool
            = CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineUpdateTask].load(std::memory_order_relaxed);
        if (busy_threads_in_pool >= max_running_update_task)
        {
            sleepForSeconds(1);
            ++waited_secs;
        }
        else
        {
            update_thread_pool->scheduleOrThrowOnError(
                [uniq_engine_writer, data_parts, current_key_index, current_delete_bitmap, begin, end]
                {
                    setThreadName("dedupFuncByPart");
                    CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineUpdateTask]++;
                    try
                    {
                        uniq_engine_writer->dedupFunctionByPart(data_parts, current_key_index, current_delete_bitmap, begin, end);
                        CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineUpdateTask]--;
                    }
                    catch (std::exception const & ex)
                    {
                        CurrentMetrics::values[CurrentMetrics::BackgroundUniqueEngineUpdateTask]--;
                        throw ex;
                    }
                });
            return;
        }
    }
    throw Exception(
        ErrorCodes::TIMEOUT_EXCEEDED,
        "Timeout while scheduling deduplication task of unique engine to thread pool, timeout is {} seconds.",
        std::to_string(timeout_in_sec));
}

void UniqueEngineDataWriter::executeParallelDedupByPart(
    const ActiveDataPartPtrs & data_parts, const UniqueKeyIndexPtr & current_key_index, const UniqueDeleteBitmapPtr & current_delete_bitmap)
{
    size_t bucket_size = static_cast<size_t>(std::ceil(data_parts.size() * 1.0 / (*storage_settings)[MergeTreeSetting::unique_key_update_parallelism]));

    LOG_DEBUG(
        log,
        "totally {} data parts,"
        "the parallelism for updating unique key is {}, and the bucket size is {}.",
        data_parts.size(),
        (*storage_settings)[MergeTreeSetting::unique_key_update_parallelism].toString(),
        bucket_size);

    if (!bucket_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "zero bucket size for parallel deduplication task by part.");

    if (!update_thread_pool)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "the update parallel pool was not initialized.");

    size_t begin = 0, end = 0;
    for (; end < data_parts.size(); ++end)
    {
        if (end - begin == bucket_size)
        {
            scheduleDedupTask(this, data_parts, current_key_index, current_delete_bitmap, begin, end);
            begin = end;
        }
    }
    scheduleDedupTask(this, data_parts, current_key_index, current_delete_bitmap, begin, end);
    update_thread_pool->wait();

    for (const auto & [data_part, _] : unique_delete_bitmap_map)
        flushToTempFiles(data_part);

    for (const auto & [data_part, deleted_keys] : unique_deleted_keys_map)
        flushToTempFiles(data_part, deleted_keys);
}

void UniqueEngineDataWriter::executeDedupByIterator(
    const ActiveDataPartPtrs & data_parts, const UniqueKeyIndexPtr & current_key_index, const UniqueDeleteBitmapPtr & current_delete_bitmap)
{
    auto current_iterator = getUniqueKeyIterator(current_key_index, current_delete_bitmap);

    if (!current_iterator->status().ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper new part iterator has error {}", current_iterator->status().ToString());

    std::vector<UniqueKeyIndexPtr> active_key_indices;
    for (const auto & active_part : data_parts)
        active_key_indices.emplace_back(active_part->getUniqueKeyIndex(false, nullptr, nullptr, true));

    IndexFileIterators active_key_iterators;
    for (size_t i = 0; i < data_parts.size(); ++i)
    {
        const auto & delete_bitmap = data_parts[i]->getUniqueDeleteBitmap();
        auto active_key_iterator = getUniqueKeyIterator(active_key_indices[i], delete_bitmap);
        active_key_iterators.emplace_back(std::move(active_key_iterator));
    }

    const IndexFile::Comparator * comparator = DB::IndexFile::BytewiseComparator();
    IndexFile::IndexFileMergeIterator merge_iterator(comparator, std::move(active_key_iterators));

    if (!merge_iterator.status().ok())
        throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper active parts iterator has error {}", merge_iterator.status().ToString());

    current_iterator->SeekToFirst();

    if (current_iterator->Valid())
    {
        merge_iterator.Seek(current_iterator->key());

        if (!merge_iterator.Valid())
        {
            LOG_DEBUG(log, "the new part has no duplicate data with active parts.");
            return;
        }
    }
    else
    {
        LOG_WARNING(log, "the new part has no data.");
        return;
    }

    std::map<String, VersionAndRow> to_update_current;
    std::map<MutableDataPartPtr, std::vector<size_t>> to_update_normal;
    std::map<MutableDataPartPtr, DeletedKeysPtr> to_update_merging_moving;

    bool rowid_is_uinit32 = (*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type] == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;

    while (current_iterator->Valid())
    {
        if (!current_iterator->status().ok())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper new part iterator has error {}", current_iterator->status().ToString());

        if (!merge_iterator.status().ok())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Deduper active parts iterator has error {}", merge_iterator.status().ToString());

        if (!merge_iterator.Valid())
        {
            /// needs to read `keys` iter to the end in order to remove duplicate keys among new parts
            LOG_DEBUG(log, "the new part has no duplicate data with active parts, will skip loop.");
            break;
        }

        int cmp = comparator->Compare(current_iterator->key(), merge_iterator.key());

        bool exact_match = false;
        if (cmp < 0)
        {
            current_iterator->Next();
            continue;
        }
        else if (cmp > 0)
        {
            merge_iterator.NextUntil(current_iterator->key(), exact_match);
        }
        else
        {
            exact_match = true;
        }

        while (exact_match)
        {
            const auto & current_key = current_iterator->key().ToString();
            auto current_rowid_version = current_iterator->value();

            UInt64 current_rowid;

            if (rowid_is_uinit32)
            {
                UInt32 current_rowid_u32;
                LevelDBUniqueKeyIndex::decodeUInt32Rowid(current_rowid_version, current_rowid_u32);
                current_rowid = current_rowid_u32;
            }
            else
                LevelDBUniqueKeyIndex::decodeUInt64Rowid(current_rowid_version, current_rowid);

            UInt64 current_version;
            LevelDBUniqueKeyIndex::decodeVersion(current_rowid_version, current_version);

            auto active_rowid_version = merge_iterator.value();

            UInt64 active_rowid;

            if (rowid_is_uinit32)
            {
                UInt32 active_rowid_u32;
                LevelDBUniqueKeyIndex::decodeUInt32Rowid(active_rowid_version, active_rowid_u32);
                active_rowid = active_rowid_u32;
            }
            else
                LevelDBUniqueKeyIndex::decodeUInt64Rowid(active_rowid_version, active_rowid);

            UInt64 active_version;
            LevelDBUniqueKeyIndex::decodeVersion(active_rowid_version, active_version);

            const auto & active_part_idx = merge_iterator.child_index();
            const auto & active_part = data_parts[active_part_idx];

            compareWithActivePart(
                to_update_current,
                to_update_normal,
                to_update_merging_moving,
                current_key,
                current_version,
                current_rowid,
                active_part,
                active_version,
                active_rowid);

            exact_match = false;
            current_iterator->Next();
            if (current_iterator->Valid())
                merge_iterator.NextUntil(current_iterator->key(), exact_match);
        }
    }

    /// Delete the duplicate rows in part_to_write and cache the updated data in delete_bitmap_map and key_indicies_map.
    /// Please note that the deleted rows still exist in part_to_write, they are only truly deleted after commit() finished.
    if (!to_update_current.empty())
    {
        enrollDataPart(part_to_write);
        for (auto & key_version_row : to_update_current)
        {
            const auto & pw_delete_bitmap = getDeleteBitmap(part_to_write);
            pw_delete_bitmap->deleteRow(std::get<1>(key_version_row.second));
        }
    }

    /// Delete the duplicate rows in every active data part and cache the updated data in delete_bitmap_map and key_indicies_map.
    /// Also please note that the deleted rows still exist in corresponding active data part, they are only truly deleted after commit() finished.
    for (const auto & pair : to_update_normal)
    {
        enrollDataPart(pair.first);
        for (const auto & row_to_delete : pair.second)
        {
            const auto & normal_delete_bitmap = getDeleteBitmap(pair.first);
            normal_delete_bitmap->deleteRow(row_to_delete);
        }
    }

    /// Flush all the deleted keys of the data parts being merged to corresponding temporary files.
    for (const auto & [data_part, deleted_keys] : to_update_merging_moving)
        enrollDataPart(data_part, deleted_keys);

    for (const auto & [data_part, _] : unique_delete_bitmap_map)
        flushToTempFiles(data_part);

    for (const auto & [data_part, deleted_keys] : unique_deleted_keys_map)
        flushToTempFiles(data_part, deleted_keys);
}

void UniqueEngineDataWriter::compareWithActivePart(
    std::map<String, VersionAndRow> & to_update_current,
    std::map<MutableDataPartPtr, std::vector<size_t>> & to_update_normal,
    std::map<MutableDataPartPtr, DeletedKeysPtr> & to_update_merging_moving,
    const String & key_str,
    const UInt64 & current_version,
    const UInt64 & current_rowid,
    const MutableDataPartPtr & active_part,
    const UInt64 & active_version,
    const UInt64 & active_rowid)
{
    if (current_version <= active_version)
        to_update_current.insert(std::make_pair(key_str, std::make_pair(current_version, current_rowid)));
    else
    {
        auto expect = IMergeTreeDataPart::MergeUpdateStatus::NORMAL;
        auto to = IMergeTreeDataPart::MergeUpdateStatus::UPDATING;

        /// As long as merge_update_status of part cannot be changed from normal to updating,
        /// part are identified as merging or moving. The purpose of doing so is to prevent conflicts between write and merge, write and move,
        /// and avoid data duplication.
        if (active_part->merge_update_status.load() != to && !active_part->merge_update_status.compare_exchange_strong(expect, to))
        {
            if (!to_update_merging_moving.contains(active_part))
                to_update_merging_moving[active_part] = std::make_shared<DeletedKeys>();
            to_update_merging_moving[active_part]->insert(std::make_pair(key_str, active_version));
        }

        to_update_normal[active_part].emplace_back(active_rowid);
    }
}

void UniqueEngineDataWriter::prepareForNewPart(bool is_merge_by_fetch)
{
    DataPartsVector active_parts_range;
    if ((*storage_settings)[MergeTreeSetting::unique_key_deduplicate_level] == UniqueEngineDataWriter::DedupType::TABLE)
        active_parts_range = storage.getDataPartsVectorForInternalUsage({MergeTreeDataPartState::Active});
    else if ((*storage_settings)[MergeTreeSetting::unique_key_deduplicate_level] == UniqueEngineDataWriter::DedupType::PARTITION)
        active_parts_range
            = storage.getDataPartsVectorInPartitionForInternalUsage(MergeTreeDataPartState::Active, part_to_write->info.getPartitionId());
    else
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Invalid level {} for setting unique_key_deduplicate_level.",
            (*storage_settings)[MergeTreeSetting::unique_key_deduplicate_level].toString());

    if (active_parts_range.empty())
        return;

    ActiveDataPartPtrs parts_to_dedup;
    for (const auto & item : active_parts_range)
    {
        if (is_merge_by_fetch && item->info.getPartitionId() == part_to_write->info.getPartitionId()
            && item->info.min_block >= part_to_write->info.min_block && item->info.max_block <= part_to_write->info.max_block)
            continue;

        if (item->effective_rows_count == 0)
        {
            LOG_DEBUG(log, "part {} effective_rows_count equal to zero, will skip directly.", item->name);
            continue;
        }

        parts_to_dedup.emplace_back(const_pointer_cast<DataPart>(item));
    }

    if (parts_to_dedup.empty())
        return;

    /// if unique_key_index_resident_in_memory = 1 and unique_key_bucket = 1, unique key of part that fetched from another replica need to be loading thoroughly.
    auto current_unique_key_index = part_to_write->getUniqueKeyIndex(true, loading_bucket_pool);
    const auto & current_unique_delete_bitmap = part_to_write->getUniqueDeleteBitmap();

    Stopwatch total_stopwatch{CLOCK_MONOTONIC_COARSE};

    const auto & unique_key_index_type = (*storage_settings)[MergeTreeSetting::unique_key_index_type];
    if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
    {
        if ((*storage_settings)[MergeTreeSetting::unique_key_update_parallel_type] == UniqueEngineDataWriter::ParallelType::DATA_PART)
            executeParallelDedupByPart(parts_to_dedup, current_unique_key_index, current_unique_delete_bitmap);
        else if ((*storage_settings)[MergeTreeSetting::unique_key_update_parallel_type] == UniqueEngineDataWriter::ParallelType::DATA_KEY)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The parallel preparation by key for unique engine is not implemented yet.");
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid value {} for setting unique_key_update_parallel_type.",
                (*storage_settings)[MergeTreeSetting::unique_key_update_parallel_type].toString());
    }
    else if (IUniqueKeyIndex::isLevelDBUniqueKeyIndex(unique_key_index_type))
        executeDedupByIterator(parts_to_dedup, current_unique_key_index, current_unique_delete_bitmap);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid type({}) for unique key index.", unique_key_index_type.toString());

    double ms = total_stopwatch.elapsedMilliseconds();
    LOG_DEBUG(
        getLogger("UniqueMergeTreeIndex"),
        "part {} is_merge_by_fetch {} dedup with active parts cost {} ms",
        part_to_write->name,
        is_merge_by_fetch,
        ms);
}

void UniqueEngineDataWriter::prepareForMergeOrMoveResultPart()
{
    std::map<String, VersionAndRow> to_update_current;

    const auto & unique_key_index_type = (*storage_settings)[MergeTreeSetting::unique_key_index_type];

    /// map unique indexes use readVarUInt, but only 2^ 63-1 is supported at most. leveldb unique indexes use readBinary directly.
    bool is_read_binary = unique_key_index_type == IUniqueKeyIndex::Type::LEVEL_DB;

    bool rowid_is_uinit32 = (*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type] == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;

    if (part_to_write->commit_type == IMergeTreeDataPart::CommitType::EXECUTE_MERGE)
    {
        for (const auto & source_part : part_to_write->merge_source_parts)
            prepareForDeleteKeys(source_part, to_update_current, is_read_binary, rowid_is_uinit32, unique_key_index_type);
    }
    else
    {
        const auto & source_part = part_to_write->move_source_part;
        prepareForDeleteKeys(source_part, to_update_current, is_read_binary, rowid_is_uinit32, unique_key_index_type);
    }

    /// Delete the duplicate rows in part_to_write and cache the updated data in unique_delete_bitmap_map.
    /// Please note that the deleted rows still exist in part_to_write, they are only truly deleted after commit() finished.
    if (!to_update_current.empty())
    {
        enrollDataPart(part_to_write);
        for (auto & key_version_row : to_update_current)
            unique_delete_bitmap_map[part_to_write]->deleteRow(std::get<1>(key_version_row.second));
        flushToTempFiles(part_to_write);
    }
}

void UniqueEngineDataWriter::prepareForDeleteKeys(
    MergeTreeDataPartPtr source_part,
    std::map<String, VersionAndRow> & to_update_current,
    const bool & is_read_binary,
    const bool & rowid_is_uinit32,
    const UInt64 & unique_key_index_type)
{
    const auto & data_part_storage = dynamic_cast<const DataPartStorageOnDiskFull &>(source_part->getDataPartStorage());
    auto disk = data_part_storage.volume->getDisk();

    const auto deleted_keys_dir_path = fs::path(data_part_storage.getFullPathWithoutLastSlash() + MERGING_MOVING_DIR_SUFFIX);
    const auto deleted_keys_file_path = deleted_keys_dir_path / DELETED_KEYS_FILE_NAME;

    if (disk->existsFile(deleted_keys_file_path))
    {
        UniqueKeyIndexPtr unique_key_index;
        if (part_to_write->commit_type == IMergeTreeDataPart::CommitType::EXECUTE_MERGE)
            unique_key_index = part_to_write->getUniqueKeyIndex();
        else
            unique_key_index = part_to_write->getUniqueKeyIndex(false, loading_bucket_pool);

        const auto & unique_key_delete_bitmap = part_to_write->getUniqueDeleteBitmap();

        auto in = disk->readFile(deleted_keys_file_path, getReadSettings());
        while (!in->eof())
        {
            DeletedKeys deleted_keys;
            deleted_keys.deserializeBinary(*in, is_read_binary);
            for (const auto & deleted_key_and_version : deleted_keys)
            {
                const auto & deleted_key = deleted_key_and_version.first;
                const auto & deleted_version = deleted_key_and_version.second;

                std::optional<VersionAndRow> current_version_and_row;
                if (IUniqueKeyIndex::isMapUniqueKeyIndex(unique_key_index_type))
                    current_version_and_row = unique_key_index->get(deleted_key);
                else if (IUniqueKeyIndex::isLevelDBUniqueKeyIndex(unique_key_index_type))
                    current_version_and_row = unique_key_index->get(deleted_key, rowid_is_uinit32);
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid type({}) for unique key index.", unique_key_index_type);

                if (current_version_and_row)
                {
                    const auto & current_version = std::get<0>(current_version_and_row.value());
                    const auto & current_row_num = std::get<1>(current_version_and_row.value());

                    if (deleted_version == current_version && !unique_key_delete_bitmap->isDeleted(current_row_num))
                        to_update_current.insert(std::make_pair(deleted_key, std::make_pair(current_version, current_row_num)));
                }
            }
        }

        disk->removeRecursive(deleted_keys_dir_path);
    }
}

void UniqueEngineDataWriter::prepareForMergeByFetchPart()
{
    /// TODO ::: optimize to avoid the heavy key search and check in this function.
    prepareForNewPart(true);
}

void UniqueEngineDataWriter::enrollDataPart(const MutableDataPartPtr & data_part, DeletedKeysPtr deleted_keys)
{
    if (containDeleteBitmap(data_part) && (!deleted_keys || containDeletedKeys(data_part)))
        return;

    addDeleteBitmap(data_part);

    if (deleted_keys)
        addDeletedKeys(data_part, deleted_keys);
}

void UniqueEngineDataWriter::commit()
{
    for (const auto & [data_part, delete_bitmap] : unique_delete_bitmap_map)
    {
        const auto & data_part_storage = dynamic_cast<const DataPartStorageOnDiskFull &>(data_part->getDataPartStorage());
        auto disk = data_part_storage.volume->getDisk();

        auto tmp_delete_bitmap_file
            = fs::path(data_part_storage.getFullPathWithoutLastSlash() + TEMP_DIR_SUFFIX) / UNIQUE_ENGINE_DELETE_BITMAP;
        auto tmp_merging_moving_deleted_keys_file
            = fs::path(data_part_storage.getFullPathWithoutLastSlash() + TEMP_MERGING_MOVING_DIR_SUFFIX) / DELETED_KEYS_FILE_NAME;

        if (disk->existsFile(tmp_delete_bitmap_file))
        {
            auto delete_bitmap_file = fs::path(data_part_storage.getFullPathWithoutLastSlash()) / UNIQUE_ENGINE_DELETE_BITMAP;
            disk->replaceFile(tmp_delete_bitmap_file, delete_bitmap_file);
        }
        if (disk->existsFile(tmp_merging_moving_deleted_keys_file))
        {
            auto merging_moving_deleted_keys_dir = fs::path(data_part_storage.getFullPathWithoutLastSlash() + MERGING_MOVING_DIR_SUFFIX);
            auto merging_moving_deleted_keys_file = fs::path(merging_moving_deleted_keys_dir) / DELETED_KEYS_FILE_NAME;
            if (!disk->existsDirectory(merging_moving_deleted_keys_dir))
            {
                disk->createDirectory(merging_moving_deleted_keys_dir);
                disk->moveFile(tmp_merging_moving_deleted_keys_file, merging_moving_deleted_keys_file);
            }
            else
                disk->replaceFile(tmp_merging_moving_deleted_keys_file, merging_moving_deleted_keys_file);
        }

        data_part->setUniqueDeleteBitmap(delete_bitmap);
        data_part->loadRowsCount();

        /// Change its merge_update_status back to NORMAL after persisting the unique data to files.
        storage.changePartMergeUpdateStatus(
            data_part, IMergeTreeDataPart::MergeUpdateStatus::UPDATING, IMergeTreeDataPart::MergeUpdateStatus::NORMAL);
    }
}

void UniqueEngineDataWriter::flushToTempFiles(const MutableDataPartPtr & data_part)
{
    const auto & data_part_storage = dynamic_cast<const DataPartStorageOnDiskFull &>(data_part->getDataPartStorage());
    auto disk = data_part_storage.volume->getDisk();

    auto tmp_dir_to_write = fs::path(data_part_storage.getFullPathWithoutLastSlash() + TEMP_DIR_SUFFIX);

    LOG_INFO(log, "temp part dir for flushing unique data is {}", tmp_dir_to_write);

    if (!disk->existsDirectory(tmp_dir_to_write))
    {
        disk->createDirectory(tmp_dir_to_write);
        has_temp_dir = true;
    }

    auto unique_delete_bitmap_out = disk->writeFile(tmp_dir_to_write / UNIQUE_ENGINE_DELETE_BITMAP);

    unique_delete_bitmap_map[data_part]->serializeBinary(*unique_delete_bitmap_out);
    unique_delete_bitmap_out->finalize();
}

void UniqueEngineDataWriter::flushToTempFiles(const MutableDataPartPtr & data_part, const DeletedKeysPtr & deleted_keys)
{
    if (!deleted_keys)
        return;

    const auto & data_part_storage = dynamic_cast<const DataPartStorageOnDiskFull &>(data_part->getDataPartStorage());
    auto disk = data_part_storage.volume->getDisk();

    auto temp_dir_to_write = fs::path(data_part_storage.getFullPathWithoutLastSlash() + TEMP_MERGING_MOVING_DIR_SUFFIX);
    auto deleted_keys_path = fs::path(data_part_storage.getFullPathWithoutLastSlash() + MERGING_MOVING_DIR_SUFFIX) / DELETED_KEYS_FILE_NAME;

    LOG_INFO(log, "temp part dir for flushing deleted keys of merging parts is {}", temp_dir_to_write);

    if (!disk->existsDirectory(temp_dir_to_write))
    {
        disk->createDirectory(temp_dir_to_write);
        has_temp_dir = true;
    }

    if (disk->existsFile(deleted_keys_path))
    {
        disk->copyFile(deleted_keys_path, *disk, (temp_dir_to_write / DELETED_KEYS_FILE_NAME), ReadSettings{});
    }

    auto out = disk->writeFile(temp_dir_to_write / DELETED_KEYS_FILE_NAME, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);

    /// map unique indexes use writeVarUInt, but only 2^ 63-1 is supported at most. leveldb unique indexes use writeBinary directly.
    bool is_write_binary = (*storage_settings)[MergeTreeSetting::unique_key_index_type] == IUniqueKeyIndex::Type::LEVEL_DB;

    deleted_keys->serializeBinary(*out, is_write_binary);
    out->finalize();
}

void UniqueEngineDataWriter::clearTempDirs()
{
    for (const auto & [data_part, _] : unique_delete_bitmap_map)
    {
        const auto & data_part_storage = dynamic_cast<const DataPartStorageOnDiskFull &>(data_part->getDataPartStorage());
        auto disk = data_part_storage.volume->getDisk();

        auto tmp_dir = fs::path(data_part_storage.getFullPathWithoutLastSlash() + TEMP_DIR_SUFFIX);
        if (disk->existsDirectory(tmp_dir))
            disk->removeRecursive(tmp_dir);

        auto tmp_merging_dir = fs::path(data_part_storage.getFullPathWithoutLastSlash() + TEMP_MERGING_MOVING_DIR_SUFFIX);
        if (disk->existsDirectory(tmp_merging_dir))
            disk->removeRecursive(tmp_merging_dir);
    }

    has_temp_dir = false;
}


bool UniqueEngineDataWriter::containDeleteBitmap(const MutableDataPartPtr & part_)
{
    std::shared_lock<std::shared_mutex> lock(unique_delete_bitmap_map_rw_lock);
    return unique_delete_bitmap_map.contains(part_);
}

UniqueDeleteBitmapPtr & UniqueEngineDataWriter::getDeleteBitmap(const MutableDataPartPtr & part_)
{
    std::shared_lock<std::shared_mutex> lock(unique_delete_bitmap_map_rw_lock);
    return unique_delete_bitmap_map[part_];
}

void UniqueEngineDataWriter::addDeleteBitmap(const MutableDataPartPtr & part_)
{
    std::unique_lock<std::shared_mutex> lock(unique_delete_bitmap_map_rw_lock);

    if (unique_delete_bitmap_map.contains(part_))
        return;

    const auto & current_unique_delete_bitmap = part_->getUniqueDeleteBitmap();

    if (current_unique_delete_bitmap)
    {
        switch ((*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type])
        {
            case IUniqueDeleteBitmap::Type::ROARING_64_BITMAP: {
                unique_delete_bitmap_map[part_] = std::make_shared<Roaring64UniqueDeleteBitmap>(
                    dynamic_cast<Roaring64UniqueDeleteBitmap &>(*current_unique_delete_bitmap));
                break;
            }
            case IUniqueDeleteBitmap::Type::ROARING_32_BITMAP: {
                unique_delete_bitmap_map[part_] = std::make_shared<Roaring32UniqueDeleteBitmap>(
                    dynamic_cast<Roaring32UniqueDeleteBitmap &>(*current_unique_delete_bitmap));
                break;
            }
            default: {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Invalid type({}) for unique delete bitmap.", (*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type].toString());
            }
        }
    }
    else
    {
        switch ((*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type])
        {
            case IUniqueDeleteBitmap::Type::ROARING_64_BITMAP: {
                unique_delete_bitmap_map[part_] = std::make_shared<Roaring64UniqueDeleteBitmap>();
                break;
            }
            case IUniqueDeleteBitmap::Type::ROARING_32_BITMAP: {
                unique_delete_bitmap_map[part_] = std::make_shared<Roaring32UniqueDeleteBitmap>();
                break;
            }
            default: {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Invalid type({}) for unique delete bitmap.", (*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type].toString());
            }
        }
    }
}

bool UniqueEngineDataWriter::containDeletedKeys(const MutableDataPartPtr & part_)
{
    std::shared_lock<std::shared_mutex> lock(unique_deleted_keys_map_rw_lock);
    return unique_deleted_keys_map.contains(part_);
}

void UniqueEngineDataWriter::addDeletedKeys(const MutableDataPartPtr & part_, const DeletedKeysPtr & deleted_keys_)
{
    std::unique_lock<std::shared_mutex> lock(unique_deleted_keys_map_rw_lock);

    if (unique_deleted_keys_map.contains(part_))
        return;

    unique_deleted_keys_map[part_] = deleted_keys_;
}

MutableDataPartPtr UniqueEngineDataWriter::getDataPart() const
{
    return part_to_write;
}

UniqueKeyIterator
UniqueEngineDataWriter::getUniqueKeyIterator(const UniqueKeyIndexPtr & key_index, const UniqueDeleteBitmapPtr & delete_bitmap)
{
    IndexFile::ReadOptions opts;
    opts.fill_cache = true;

    if (delete_bitmap->deleteRowsSize())
    {
        bool rowid_is_uinit32 = (*storage_settings)[MergeTreeSetting::unique_delete_bitmap_type] == IUniqueDeleteBitmap::Type::ROARING_32_BITMAP;

        opts.select_predicate = [rowid_is_uinit32, delete_bitmap](const Slice &, const Slice & val)
        {
            Slice input = val;
            /// TODO: handle corrupt data in a better way.
            /// E.g., make select_predicate return Status in order to propogate the error to the client

            bool deleted;
            if (rowid_is_uinit32)
            {
                UInt32 rowid;
                deleted = LevelDBUniqueKeyIndex::decodeUInt32Rowid(input, rowid) && delete_bitmap->isDeleted(rowid);
            }
            else
            {
                UInt64 rowid;
                deleted = LevelDBUniqueKeyIndex::decodeUInt64Rowid(input, rowid) && delete_bitmap->isDeleted(rowid);
            }
            return !deleted;
        };
    }

    return key_index->newIterator(opts);
}
}
