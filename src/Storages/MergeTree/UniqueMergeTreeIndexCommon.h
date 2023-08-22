#pragma once

#include <map>
#include <memory>
#include <utility>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/BloomFilterHash.h>
#include <Storages/MergeTree/UniqueMergeTreeIndex.h>
#include <base/logger_useful.h>
#include <base/scope_guard_safe.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include <Common/CurrentThread.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>

namespace DB
{
struct DeletedKeys : std::map<String, UInt64>
{
    void serializeBinary(WriteBuffer & ostr) const;
    void deserializeBinary(ReadBuffer & istr);
};

using DeletedKeysPtr = std::shared_ptr<DeletedKeys>;

/// Mark deleted row, and used in query.
struct Roaring64UniqueDeleteBitmap : public IUniqueDeleteBitmap
{
    Roaring64UniqueDeleteBitmap() = default;
    Roaring64UniqueDeleteBitmap(Roaring64UniqueDeleteBitmap & other);

    void deleteRow(size_t pos) override;
    bool isDeleted(size_t pos) const override;
    size_t deleteRowsSize() override;
    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

private:
    roaring::Roaring64Map rb;
};

/// Mark deleted row, and used in query.
struct Roaring32UniqueDeleteBitmap : public IUniqueDeleteBitmap
{
    Roaring32UniqueDeleteBitmap() = default;
    Roaring32UniqueDeleteBitmap(Roaring32UniqueDeleteBitmap & other);

    void deleteRow(size_t pos) override;
    bool isDeleted(size_t pos) const override;
    size_t deleteRowsSize() override;
    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;

private:
    roaring::Roaring rb;
};

struct UniqueKeyMinMaxIndex
{
    UniqueKeyMinMaxIndex() = default;

    void setMinMax(const String & min_, const String & max_);
    String getMin() const;
    String getMax() const;
    void serializeBinary(WriteBuffer & ostr) const;
    void deserializeBinary(ReadBuffer & istr);

private:
    String min;
    String max;
};

using UniqueKeyMinMaxIndexPtr = std::shared_ptr<UniqueKeyMinMaxIndex>;

struct UniqueKeyBucketIndex : IUniqueKeyBucketIndex
{
    UniqueKeyBucketIndex() = default;

    void init(const size_t & bucket_size_, const size_t & rows_count_) override;
    void add(const UniqueKeyBucketInfo & bucket_info) override;
    size_t getBucketSize() const override;
    size_t getBucketNum() const override;
    UniqueKeyBucketInfos getBucketInfos() const override;
    void serializeBinary(WriteBuffer & ostr) const override;
    void deserializeBinary(ReadBuffer & istr) override;
};

struct StringHashMapUniqueKeyIndex : public IUniqueKeyIndex
{
    using UniqueKeyIndexMapType = StringHashMap<VersionAndRow>;
    using UniqueKeyIndexMapTypePtr = std::shared_ptr<UniqueKeyIndexMapType>;
    using UniqueKeyIndexBucketType = std::vector<UniqueKeyIndexMapTypePtr>;

    StringHashMapUniqueKeyIndex() = default;

    void initBucket(const size_t & bucket_num_) override;
    void add(const String & key, const VersionAndRow & value) override;
    bool empty() const override;
    size_t size() const override;
    void forEach(std::function<void(const StringRef &, const VersionAndRow &)> func) override;
    std::optional<VersionAndRow> get(const String & key) const override;
    std::optional<size_t> getRowNumber(const String & key) const override;
    std::optional<UInt64> getRowVersion(const String & key) const override;
    std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const override;
    void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const override;
    void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index,
        LoadingBucketPoolPtr loading_bucket_pool,
        BucketIndexRangePtr bucket_range,
        size_t max_running_loading_task,
        size_t timeout_in_sec) override;

private:
    mutable UniqueKeyIndexMapType unique_key_index;
    mutable UniqueKeyIndexBucketType unique_key_index_bucket;
};

struct StandardMapUniqueKeyIndex : public IUniqueKeyIndex
{
    using UniqueKeyIndexMapType = std::map<String, VersionAndRow>;
    using UniqueKeyIndexBucketType = std::vector<UniqueKeyIndexMapType>;

    StandardMapUniqueKeyIndex() = default;

    void initBucket(const size_t & bucket_num_) override;
    void add(const String & key, const VersionAndRow & value) override;
    bool empty() const override;
    size_t size() const override;
    void forEach(std::function<void(const StringRef &, const VersionAndRow &)> func) override;
    std::optional<VersionAndRow> get(const String & key) const override;
    std::optional<size_t> getRowNumber(const String & key) const override;
    std::optional<UInt64> getRowVersion(const String & key) const override;
    std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const override;
    void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const override;
    void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index,
        LoadingBucketPoolPtr loading_bucket_pool,
        BucketIndexRangePtr bucket_range,
        size_t max_running_loading_task,
        size_t timeout_in_sec) override;

private:
    mutable UniqueKeyIndexMapType unique_key_index;
    mutable UniqueKeyIndexBucketType unique_key_index_bucket;
};

struct StandardUnOrderedMapUniqueKeyIndex : public IUniqueKeyIndex
{
    using UniqueKeyIndexMapType = std::unordered_map<String, VersionAndRow>;
    using UniqueKeyIndexBucketType = std::vector<UniqueKeyIndexMapType>;

    StandardUnOrderedMapUniqueKeyIndex() = default;

    void initBucket(const size_t & bucket_num_) override;
    void add(const String & key, const VersionAndRow & value) override;
    bool empty() const override;
    size_t size() const override;
    void forEach(std::function<void(const StringRef &, const VersionAndRow &)> func) override;
    std::optional<VersionAndRow> get(const String & key) const override;
    std::optional<size_t> getRowNumber(const String & key) const override;
    std::optional<UInt64> getRowVersion(const String & key) const override;
    std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const override;
    void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const override;
    void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index,
        LoadingBucketPoolPtr loading_bucket_pool,
        BucketIndexRangePtr bucket_range,
        size_t max_running_loading_task,
        size_t timeout_in_sec) override;

private:
    mutable UniqueKeyIndexMapType unique_key_index;
    mutable UniqueKeyIndexBucketType unique_key_index_bucket;
};

}
