#pragma once

#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <optional>
#include <city.h>
#include <Core/SortDescription.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/sortBlock.h>
#include <Storages/IndexFile/FilterPolicy.h>
#include <Storages/IndexFile/IndexFileMergeIterator.h>
#include <Storages/IndexFile/IndexFileReader.h>
#include <Storages/IndexFile/IndexFileWriter.h>
#include <Storages/IndexFile/Options.h>
#include <base/StringRef.h>
#include <base/sleep.h>
#include <base/types.h>
#include <rocksdb/db.h>
#include <Common/Coding.h>
#include <Common/CurrentMetrics.h>
#include <Common/ErrorCodes.h>
#include <Common/ThreadPool.h>

namespace CurrentMetrics
{
extern const Metric BackgroundUniqueEngineLoadTask;
}

namespace DB
{
namespace ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
extern const int CANNOT_OPEN_FILE;
extern const int UNKNOWN_EXCEPTION;
}

constexpr static auto UNIQUE_ENGINE_KEY_INDEX = "unique_key_index";
constexpr static auto UNIQUE_ENGINE_KEY_BUCKET_INDEX = "unique_key_bucket_index";
constexpr static auto UNIQUE_ENGINE_DELETE_BITMAP = "unique_delete_bitmap";
constexpr static auto UNIQUE_ENGINE_KEY_MINMAX_INDEX = "unique_key_minmax_index";
constexpr static auto UNIQUE_VIRTUAL_KEY_COLUMN_NAME = "_unique_key";
constexpr static auto UNIQUE_VIRTUAL_VERSION_COLUMN_NAME = "_unique_version";
constexpr static auto UNIQUE_VIRTUAL_ROWID_COLUMN_NAME = "_unique_rowid";

/// Mark deleted row, and used in query.
struct IUniqueDeleteBitmap
{
    enum Type
    {
        ROARING_64_BITMAP = 64,
        ROARING_32_BITMAP = 32
    };

    virtual ~IUniqueDeleteBitmap() = default;

    virtual void deleteRow(size_t pos) = 0;
    virtual bool isDeleted(size_t pos) const = 0;
    virtual size_t deleteRowsSize() = 0;
    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;
};

using UniqueDeleteBitmapPtr = std::shared_ptr<IUniqueDeleteBitmap>;
using UniqueKeyIterator = std::unique_ptr<IndexFile::Iterator>;

class CountDownLatch
{
public:
    explicit CountDownLatch(size_t count) : m_count(count) { }

    void countDown() noexcept
    {
        std::lock_guard<std::mutex> guard(m_mutex);
        if (0 == m_count)
            return;

        --m_count;
        if (0 == m_count)
            m_cv.notify_all();
    }

    void await() noexcept
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cv.wait(lock, [this] { return 0 == m_count; });
    }

private:
    std::mutex m_mutex;
    std::condition_variable m_cv;
    size_t m_count;
};

using VersionAndRow = std::tuple<UInt64, size_t>;

struct UniqueKeyBucketInfo
{
    UniqueKeyBucketInfo() = default;
    UniqueKeyBucketInfo(size_t offset_in_file_, size_t rows_);

    size_t getOffsetInFile() const;
    size_t getRows() const;

private:
    size_t offset_in_file;
    size_t rows;
};

using UniqueKeyBucketInfos = std::vector<UniqueKeyBucketInfo>;

struct IUniqueKeyBucketIndex
{
    virtual ~IUniqueKeyBucketIndex() = default;

    virtual void init(size_t bucket_size_, size_t rows_count_) = 0;

    virtual void add(const UniqueKeyBucketInfo & bucket_info) = 0;

    virtual size_t getBucketSize() const = 0;

    virtual size_t getBucketNum() const = 0;

    virtual UniqueKeyBucketInfos getBucketInfos() const = 0;

    virtual void serializeBinary(WriteBuffer & ostr) const = 0;

    virtual void deserializeBinary(ReadBuffer & istr) = 0;

protected:
    size_t bucket_size;
    size_t bucket_num;
    UniqueKeyBucketInfos bucket_infos;
};

class IDataPartStorage;

using UniqueKeyBucketIndexPtr = std::shared_ptr<IUniqueKeyBucketIndex>;
using UpdateThreadPoolPtr = std::shared_ptr<ThreadPool>;
using LoadingBucketPoolPtr = std::shared_ptr<ThreadPool>;
using BucketIndexRangePtr = std::shared_ptr<std::vector<size_t>>;

/// Help to find row number via specified key
struct IUniqueKeyIndex
{
    enum Type
    {
        STANDARD_MAP,
        STANDARD_UNORDERED_MAP,
        STRING_HASH_MAP,
        LEVEL_DB
    };

    virtual ~IUniqueKeyIndex() = default;

    virtual void initBucket(size_t /*bucket_num_*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual void add(const String & /*key*/, const VersionAndRow & /*value*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual bool empty() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index."); }

    virtual size_t size() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index."); }

    virtual void forEach(std::function<void(const StringRef &, const VersionAndRow &)> /*func*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual std::optional<VersionAndRow> get(const String & /*key*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual std::optional<VersionAndRow> get(const String & /*key*/, bool /*rowid_is_uinit32*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for map index.");
    }

    virtual std::vector<size_t> calculateTargetBuckets(size_t /*mod_bucket_num*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual void serializeBinary(WriteBuffer & /*ostr*/, UniqueKeyBucketIndexPtr /*bucket_index*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual void serializeBinary(
        const String & /*index_path*/,
        Block & /*block*/,
        const UniqueDeleteBitmapPtr & /*delete_bitmap*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        bool /*rowid_is_uinit32*/,
        bool /*is_same_key*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for map index.");
    }

    virtual void serializeBinary(
        const String & /*index_path*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const String & /*tmp_rocksdb_index_dir*/,
        std::unique_ptr<rocksdb::DB> & /*tmp_rocksdb_index_writer*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for map index.");
    }

    virtual void deserializeBinary(
        const IDataPartStorage & /*data_part_storage*/,
        const String & /*index_path*/,
        UniqueKeyBucketIndexPtr /*bucket_index*/,
        LoadingBucketPoolPtr /*loading_bucket_pool*/,
        BucketIndexRangePtr /*bucket_range*/,
        size_t /*max_running_loading_task*/,
        size_t /*timeout_in_sec*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for leveldb index.");
    }

    virtual void deserializeBinary(const String & /*index_path*/, UniqueKeyIndexBlockCachePtr /*block_cache*/)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for map index.");
    }

    /// Return an iterator over KVs in this file.
    /// Note: client should make sure the UniqueKeyIndex object lives longer than the returned iterator.
    virtual UniqueKeyIterator newIterator(const IndexFile::ReadOptions & /*options*/) const
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for map index.");
    }

    virtual size_t residentMemoryUsage() const { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method is not supported for map index."); }

    static bool isMapUniqueKeyIndex(size_t unique_key_index_type);

    static bool isLevelDBUniqueKeyIndex(size_t unique_key_index_type);

protected:
    std::mutex bucket_load_mutex;
    bool bucketing_enabled = false;
    size_t bucket_num;
};

using UniqueKeyIndexPtr = std::shared_ptr<IUniqueKeyIndex>;

}
