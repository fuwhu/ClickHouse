#pragma once


#include <grpcpp/grpcpp.h>
#include <rocksdb/db.h>
#include <dictionary_api.grpc.pb.h>

#include <Core/ExternalResultDescription.h>
#include <Processors/Sources/SourceWithProgress.h>

using dictionary::api::Dictionary;

namespace DB
{
class IBitmapDictionaryStorage
{
public:
    virtual std::vector<UInt64> getValues(const std::vector<StringRef> & keys, size_t start, size_t size) = 0;

    virtual std::vector<String> getKeys(const std::vector<UInt64> & values, size_t start, size_t size) = 0;

    virtual void clean() { }

protected:
    IBitmapDictionaryStorage() = default;
    virtual ~IBitmapDictionaryStorage() = default;
};

using BitmapDictionaryStoragePtr = std::shared_ptr<IBitmapDictionaryStorage>;

class BitmapDictionaryStorageRocksDB final : public IBitmapDictionaryStorage
{
public:
    explicit BitmapDictionaryStorageRocksDB(
        const String & dict_,
        const String & local_path_,
        const std::shared_ptr<rocksdb::Cache> & lru_cache_,
        BitmapDictionaryStoragePtr fallback_ = nullptr);

    ~BitmapDictionaryStorageRocksDB() override;

    std::vector<UInt64> getValues(const std::vector<StringRef> & keys, size_t start, size_t size) override;

    std::vector<String> getKeys(const std::vector<UInt64> & values, size_t start, size_t size) override;

    void clean() override;

private:
    void initDB(const std::shared_ptr<rocksdb::Cache> & lru_cache);

    void closeDB(bool clean);

    Poco::Logger * log;

    const String dict;
    const String local_path;
    BitmapDictionaryStoragePtr fallback;

    std::unique_ptr<rocksdb::DB> rocksdb;
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
};

class BitmapDictionaryStorageRemote final : public IBitmapDictionaryStorage
{
public:
    BitmapDictionaryStorageRemote(const String & dict_, const String & target_);

    std::vector<UInt64> getValues(const std::vector<StringRef> & keys, size_t start, size_t size) override;

    std::vector<String> getKeys(const std::vector<UInt64> & values, size_t start, size_t size) override;

private:
    void initClient();

    Poco::Logger * log;

    const String dict;
    const String target;

    std::unique_ptr<Dictionary::Stub> stub;
};

class BitmapSource final : public SourceWithProgress
{
public:
    explicit BitmapSource(
        BitmapDictionaryStoragePtr storage_,
        const DB::Block & sample_block_,
        std::vector<StringRef> keys_,
        std::vector<UInt64> values_,
        size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

    ~BitmapSource() override = default;

    String getName() const override { return "Bitmap"; }

private:
    Chunk generate() override;

    BitmapDictionaryStoragePtr storage;
    std::vector<StringRef> keys;
    std::vector<UInt64> values;

    ExternalResultDescription description;

    const size_t max_block_size;
    size_t cursor = 0;
    bool all_read = false;
};
}
