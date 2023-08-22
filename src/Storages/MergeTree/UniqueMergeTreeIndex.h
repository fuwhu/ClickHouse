#pragma once

#include <condition_variable>
#include <mutex>
#include <optional>
#include <city.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/WriteBuffer.h>
#include <base/StringRef.h>
#include <base/sleep.h>
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
}

constexpr static auto UNIQUE_ENGINE_KEY_INDEX = "unique_key_index";
constexpr static auto UNIQUE_ENGINE_KEY_BUCKET_INDEX = "unique_key_bucket_index";
constexpr static auto UNIQUE_ENGINE_DELETE_BITMAP = "unique_delete_bitmap";
constexpr static auto UNIQUE_ENGINE_KEY_MINMAX_INDEX = "unique_key_minmax_index";

/// Mark deleted row, and used in query.
struct IUniqueDeleteBitmap
{
    virtual ~IUniqueDeleteBitmap() = default;

    virtual void deleteRow(size_t pos) = 0;
    virtual bool isDeleted(size_t pos) const = 0;
    virtual size_t deleteRowsSize() = 0;
    virtual void serializeBinary(WriteBuffer & ostr) const = 0;
    virtual void deserializeBinary(ReadBuffer & istr) = 0;
};

using UniqueDeleteBitmapPtr = std::shared_ptr<IUniqueDeleteBitmap>;

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
    UniqueKeyBucketInfo(const size_t & offset_in_file_, const size_t & rows_);

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

    virtual void init(const size_t & bucket_size_, const size_t & rows_count_) = 0;
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

using UniqueKeyBucketIndexPtr = std::shared_ptr<IUniqueKeyBucketIndex>;
using LoadingBucketPoolPtr = std::shared_ptr<ThreadPool>;
using BucketIndexRangePtr = std::shared_ptr<std::vector<size_t>>;

/// Help to find row number via specified key
struct IUniqueKeyIndex
{
    virtual ~IUniqueKeyIndex() = default;

    virtual void initBucket(const size_t & bucket_num_) = 0;
    virtual void add(const String & key, const VersionAndRow & value) = 0;
    virtual bool empty() const = 0;
    virtual size_t size() const = 0;
    virtual void forEach(std::function<void(const StringRef &, const VersionAndRow &)> func) = 0;

    virtual std::optional<VersionAndRow> get(const String & key) const = 0;
    virtual std::optional<size_t> getRowNumber(const String & key) const = 0;
    virtual std::optional<UInt64> getRowVersion(const String & key) const = 0;
    virtual std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const = 0;

    virtual void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index = nullptr) const = 0;
    virtual void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index = nullptr,
        LoadingBucketPoolPtr loading_bucket_pool = nullptr,
        BucketIndexRangePtr bucket_range = nullptr,
        size_t max_running_loading_task = 0,
        size_t timeout_in_sec = 0)
    = 0;

protected:
    std::mutex bucket_load_mutex;
    bool bucketing_enabled = false;
    size_t bucket_num;
};

using UniqueKeyIndexPtr = std::shared_ptr<IUniqueKeyIndex>;

}
