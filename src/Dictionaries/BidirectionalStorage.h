#pragma once

#include <Common/config.h>

#if USE_GRPC
#    include <base/StringRef.h>
#    include <base/types.h>
#    include <grpcpp/grpcpp.h>
#    include <rocksdb/db.h>
#    include <dictionary_api.grpc.pb.h>
#    include <Poco/Logger.h>

namespace DB
{
class IBidirectionalStorage
{
public:
    virtual std::vector<UInt64> getValues(const std::vector<StringRef> & keys, size_t start, size_t size) = 0;

    virtual std::vector<String> getKeys(const std::vector<UInt64> & values, size_t start, size_t size) = 0;

    virtual void clean() { }

protected:
    IBidirectionalStorage() = default;
    virtual ~IBidirectionalStorage() = default;
};

using BidirectionalStoragePtr = std::shared_ptr<IBidirectionalStorage>;

class BidirectionalStorageRocksDB final : public IBidirectionalStorage
{
public:
    explicit BidirectionalStorageRocksDB(
        const String & dict_, const String & uuid_, const String & local_base_path_, BidirectionalStoragePtr fallback_);

    ~BidirectionalStorageRocksDB() override;

    std::vector<UInt64> getValues(const std::vector<StringRef> & keys, size_t start, size_t size) override;

    std::vector<String> getKeys(const std::vector<UInt64> & values, size_t start, size_t size) override;

    void clean() override;

private:
    void initDB();

    void closeDB(bool clean);

    Poco::Logger * log;

    const String dict;
    const String uuid;
    const String local_base_path;
    BidirectionalStoragePtr fallback;

    std::unique_ptr<rocksdb::DB> rocksdb;
    std::vector<rocksdb::ColumnFamilyHandle *> handles;
};

class BidirectionalStorageRemote final : public IBidirectionalStorage
{
public:
    BidirectionalStorageRemote(const String & dict_, const String & target_);

    std::vector<UInt64> getValues(const std::vector<StringRef> & keys, size_t start, size_t size) override;

    std::vector<String> getKeys(const std::vector<UInt64> & values, size_t start, size_t size) override;

private:
    void initClient();

    void checkDict();

    Poco::Logger * log;

    const String dict;
    const String target;

    std::unique_ptr<dictionary::api::Dictionary::Stub> stub;
};
}
#endif
