#pragma once

#include <map>
#include <memory>
#include <utility>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/BloomFilter.h>
#include <Interpreters/BloomFilterHash.h>
#include <Storages/MergeTree/UniqueMergeTreeIndex.h>
#include <base/logger_useful.h>
#include <base/scope_guard_safe.h>
#include <roaring.hh>
#include <roaring64map.hh>
#include <Common/Coding.h>
#include <Common/CurrentThread.h>
#include <Common/HashTable/StringHashMap.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>

namespace DB
{
struct DeletedKeys : std::map<String, UInt64>
{
    void serializeBinary(WriteBuffer & ostr, const bool & is_write_binary) const;
    void deserializeBinary(ReadBuffer & istr, const bool & is_read_binary);
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
    std::optional<VersionAndRow> get(const String & /*key*/, const bool & /*rowid_is_uinit32*/) const override { return {}; }
    std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const override;
    void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const override;
    void serializeBinary(
        const String & /*index_path*/,
        Block & /*block*/,
        const UniqueDeleteBitmapPtr & /*delete_bitmap*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const bool & /*rowid_is_uinit32*/,
        const bool & /*is_same_key*/) const override
    {
    }
    void serializeBinary(
        const String & /*index_path*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const String & /*temp_unique_key_index_dir*/,
        std::unique_ptr<rocksdb::DB> & /*temp_unique_key_index*/) const override
    {
    }
    void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index,
        LoadingBucketPoolPtr loading_bucket_pool,
        BucketIndexRangePtr bucket_range,
        size_t max_running_loading_task,
        size_t timeout_in_sec) override;
    void deserializeBinary(const String & /*file_path*/, UniqueKeyIndexBlockCachePtr /*block_cache*/) override { }
    UniqueKeyIterator newIterator(const IndexFile::ReadOptions & /*options*/) const override { return nullptr; }
    size_t residentMemoryUsage() const override { return 0; }

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
    std::optional<VersionAndRow> get(const String & /*key*/, const bool & /*rowid_is_uinit32*/) const override { return {}; }
    std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const override;
    void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const override;
    void serializeBinary(
        const String & /*index_path*/,
        Block & /*block*/,
        const UniqueDeleteBitmapPtr & /*delete_bitmap*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const bool & /*rowid_is_uinit32*/,
        const bool & /*is_same_key*/) const override
    {
    }
    void serializeBinary(
        const String & /*index_path*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const String & /*temp_unique_key_index_dir*/,
        std::unique_ptr<rocksdb::DB> & /*temp_unique_key_index*/) const override
    {
    }
    void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index,
        LoadingBucketPoolPtr loading_bucket_pool,
        BucketIndexRangePtr bucket_range,
        size_t max_running_loading_task,
        size_t timeout_in_sec) override;
    void deserializeBinary(const String & /*file_path*/, UniqueKeyIndexBlockCachePtr /*block_cache*/) override { }
    UniqueKeyIterator newIterator(const IndexFile::ReadOptions & /*options*/) const override { return nullptr; }
    size_t residentMemoryUsage() const override { return 0; }

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
    std::optional<VersionAndRow> get(const String & /*key*/, const bool & /*rowid_is_uinit32*/) const override { return {}; }
    std::vector<size_t> calculateTargetBuckets(const size_t & mod_bucket_num) const override;
    void serializeBinary(WriteBuffer & ostr, UniqueKeyBucketIndexPtr bucket_index) const override;
    void serializeBinary(
        const String & /*index_path*/,
        Block & /*block*/,
        const UniqueDeleteBitmapPtr & /*delete_bitmap*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const bool & /*rowid_is_uinit32*/,
        const bool & /*is_same_key*/) const override
    {
    }
    void serializeBinary(
        const String & /*index_path*/,
        IndexFile::IndexFileInfo & /*file_info*/,
        const String & /*temp_unique_key_index_dir*/,
        std::unique_ptr<rocksdb::DB> & /*temp_unique_key_index*/) const override
    {
    }
    void deserializeBinary(
        const DiskPtr & disk,
        const String & index_path,
        UniqueKeyBucketIndexPtr bucket_index,
        LoadingBucketPoolPtr loading_bucket_pool,
        BucketIndexRangePtr bucket_range,
        size_t max_running_loading_task,
        size_t timeout_in_sec) override;
    void deserializeBinary(const String & /*file_path*/, UniqueKeyIndexBlockCachePtr /*block_cache*/) override { }
    UniqueKeyIterator newIterator(const IndexFile::ReadOptions & /*options*/) const override { return nullptr; }
    size_t residentMemoryUsage() const override { return 0; }

private:
    mutable UniqueKeyIndexMapType unique_key_index;
    mutable UniqueKeyIndexBucketType unique_key_index_bucket;
};

struct LevelDBUniqueKeyIndex : public IUniqueKeyIndex
{
    using IndexFileReaderType = std::unique_ptr<IndexFile::IndexFileReader>;

    LevelDBUniqueKeyIndex() = default;

    void initBucket(const size_t & /*bucket_num_*/) override { }
    void add(const String & /*key*/, const VersionAndRow & /*value*/) override { }
    bool empty() const override { return false; }
    size_t size() const override { return 0; }
    void forEach(std::function<void(const StringRef &, const VersionAndRow &)> /*func*/) override { }
    std::optional<VersionAndRow> get(const String & /*key*/) const override { return {}; }
    std::optional<VersionAndRow> get(const String & key, const bool & rowid_is_uinit32) const override;
    std::vector<size_t> calculateTargetBuckets(const size_t & /*mod_bucket_num*/) const override { return {}; }
    void serializeBinary(WriteBuffer & /*ostr*/, UniqueKeyBucketIndexPtr /*bucket_index*/) const override { }
    void serializeBinary(
        const String & index_path,
        Block & block,
        const UniqueDeleteBitmapPtr & delete_bitmap,
        IndexFile::IndexFileInfo & file_info,
        const bool & rowid_is_uinit32,
        const bool & is_same_key) const override;
    void serializeBinary(
        const String & index_path,
        IndexFile::IndexFileInfo & file_info,
        const String & temp_unique_key_index_dir,
        std::unique_ptr<rocksdb::DB> & temp_unique_key_index) const override;
    void deserializeBinary(
        const DiskPtr & /*disk*/,
        const String & /*index_path*/,
        UniqueKeyBucketIndexPtr /*bucket_index*/,
        LoadingBucketPoolPtr /*loading_bucket_pool*/,
        BucketIndexRangePtr /*bucket_range*/,
        size_t /*max_running_loading_task*/,
        size_t /*timeout_in_sec*/) override
    {
    }
    void deserializeBinary(const String & file_path, UniqueKeyIndexBlockCachePtr block_cache) override;
    UniqueKeyIterator newIterator(const IndexFile::ReadOptions & options) const override;
    size_t residentMemoryUsage() const override;
    static bool decodeUInt32Rowid(Slice & input, UInt32 & rowid);
    static bool decodeUInt64Rowid(Slice & input, UInt64 & rowid);
    static bool decodeVersion(Slice & input, UInt64 & version);

private:
    IndexFileReaderType index_reader;
};

}
