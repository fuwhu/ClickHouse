#pragma once


#include "UniqueMergeTreeIndex.h"
#include "UniqueMergeTreeIndexCommon.h"

#include <mutex>
#include <vector>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Common/CurrentMetrics.h>


namespace CurrentMetrics
{
extern const Metric BackgroundUniqueEngineUpdateTask;
}

namespace DB
{

class MergeTreeData;
using DataPart = IMergeTreeDataPart;
using MutableDataPartPtr = std::shared_ptr<DataPart>;
using DataPartPtr = std::shared_ptr<const DataPart>;
using DataPartsVector = std::vector<DataPartPtr>;
struct DataPartsLock;

/// Responsible for wrapping and writing all the data specific for Unique Engine table,
/// including delete bitmap, unique key index, etc.
/// Generated for each data part of Unique Engine table being written.
class UniqueEngineDataWriter
{
public:
    constexpr static auto TEMP_DIR_SUFFIX = "_uniq_tmp";
    constexpr static auto TEMP_MERGING_MOVING_DIR_SUFFIX = "_uniq_merging_moving_tmp";
    constexpr static auto TEMP_MERGING_STAGE_DIR_SUFFIX = "_uniq_merging_stage_tmp";
    constexpr static auto MERGING_MOVING_DIR_SUFFIX = "_uniq_merging_moving";
    constexpr static auto DELETED_KEYS_FILE_NAME = "deletekeyversion";

    using ActiveDataPartPtrs = std::vector<MutableDataPartPtr>;
    using PartToWriteLock = std::unique_lock<std::mutex>;
    using IndexFileReaderPtr = std::unique_ptr<DB::IndexFile::IndexFileReader>;
    using IndexFileReaders = std::vector<IndexFileReaderPtr>;
    using IndexFileIteratorPtr = std::unique_ptr<DB::IndexFile::Iterator>;
    using IndexFileIterators = std::vector<IndexFileIteratorPtr>;

    enum DedupType
    {
        TABLE,
        PARTITION
    };

    enum ParallelType
    {
        DATA_PART,
        DATA_KEY
    };

    explicit UniqueEngineDataWriter(const MutableDataPartPtr & data_part_);

    void prepare();

    void commit();

    void clearTempDirs();

    ~UniqueEngineDataWriter();

    void scheduleDedupTask(
        UniqueEngineDataWriter * uniq_engine_writer,
        const ActiveDataPartPtrs & data_parts,
        const UniqueKeyIndexPtr & current_key_index,
        const UniqueDeleteBitmapPtr & current_delete_bitmap,
        size_t begin,
        size_t end);

    MutableDataPartPtr getDataPart() const;

private:
    void prepareForNewPart(bool is_merge_by_fetch = false);

    void prepareForMergeOrMoveResultPart();

    void prepareForDeleteKeys(
        DataPartPtr source_part,
        std::map<String, VersionAndRow> & to_update_current,
        const bool & is_read_binary,
        const bool & rowid_is_uinit32,
        const UInt64 & unique_key_index_type);

    void prepareForMergeByFetchPart();

    void enrollDataPart(const MutableDataPartPtr & data_part, DeletedKeysPtr deleted_keys = nullptr);

    // void enrollDataPart(const MutableDataPartPtr & data_part, DeletedKeysUIntVersionPtr deleted_keys = nullptr);
    void flushToTempFiles(const MutableDataPartPtr & data_part);

    void flushToTempFiles(const MutableDataPartPtr & data_part, const DeletedKeysPtr & deleted_keys);

    void executeParallelDedupByPart(
        const ActiveDataPartPtrs & data_parts,
        const UniqueKeyIndexPtr & current_key_index,
        const UniqueDeleteBitmapPtr & current_delete_bitmap);

    void executeDedupByIterator(
        const ActiveDataPartPtrs & data_parts,
        const UniqueKeyIndexPtr & current_key_index,
        const UniqueDeleteBitmapPtr & current_delete_bitmap);

    void runParallelDedupByKey();

    void dedupFunctionByPart(
        const ActiveDataPartPtrs & data_parts,
        const UniqueKeyIndexPtr & current_key_index,
        const UniqueDeleteBitmapPtr & current_delete_bitmap,
        size_t begin,
        size_t end);

    void dedupFunctionBykey();

    static void compareWithActivePart(
        std::map<String, VersionAndRow> & to_update_current,
        std::map<MutableDataPartPtr, std::vector<size_t>> & to_update_normal,
        std::map<MutableDataPartPtr, DeletedKeysPtr> & to_update_merging_moving,
        const String & key_str,
        const UInt64 & current_version,
        const UInt64 & current_rowid,
        const MutableDataPartPtr & active_part,
        const UInt64 & active_version,
        const UInt64 & active_rowid);

    PartToWriteLock lockPartToWrite() const { return PartToWriteLock(part_to_write_mutex); }

    bool containDeleteBitmap(const MutableDataPartPtr & part_);

    UniqueDeleteBitmapPtr & getDeleteBitmap(const MutableDataPartPtr & part_);

    void addDeleteBitmap(const MutableDataPartPtr & part_);

    bool containDeletedKeys(const MutableDataPartPtr & part_);

    void addDeletedKeys(const MutableDataPartPtr & part_, const DeletedKeysPtr & deleted_keys_);

    UniqueKeyIterator getUniqueKeyIterator(const UniqueKeyIndexPtr & key_index, const UniqueDeleteBitmapPtr & delete_bitmap);

    MutableDataPartPtr part_to_write;
    const MergeTreeData & storage;
    MergeTreeSettingsPtr storage_settings;
    const Settings & settings;

    std::shared_mutex unique_deleted_keys_map_rw_lock;
    std::map<MutableDataPartPtr, DeletedKeysPtr> unique_deleted_keys_map;

    std::shared_mutex unique_delete_bitmap_map_rw_lock;
    std::map<MutableDataPartPtr, UniqueDeleteBitmapPtr> unique_delete_bitmap_map;

    bool has_temp_dir = false;
    mutable std::mutex part_to_write_mutex;
    UpdateThreadPoolPtr update_thread_pool;
    LoadingBucketPoolPtr loading_bucket_pool;

    Poco::LoggerPtr log;
};

}
