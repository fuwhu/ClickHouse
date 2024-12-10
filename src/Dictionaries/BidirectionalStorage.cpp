#include "BidirectionalStorage.h"

#if USE_GRPC
#    include <filesystem>
#    include <IO/ReadHelpers.h>
#    include <base/logger_useful.h>
#    include <base/range.h>
#    include <rocksdb/table.h>

using dictionary::api::DictMetaReq;
using dictionary::api::DictMetaResp;
using dictionary::api::GetKeyReq;
using dictionary::api::GetValueReq;
using dictionary::api::KeyValueResp;


namespace DB
{
namespace ErrorCodes
{
    extern const int GRPC_ERROR;
    extern const int ROCKSDB_ERROR;
    extern const int BAD_ARGUMENTS;
}

static constexpr size_t MAX_PROXY_BATCH_SIZE = 1000;
static constexpr size_t MAX_PROXY_TIMEOUT_SECONDS = 5;

BidirectionalStorageRocksDB::BidirectionalStorageRocksDB(
    const String & dict_, const String & uuid_, const String & local_base_path_, BidirectionalStoragePtr fallback_)
    : log(&Poco::Logger::get("BidirectionalStorageRocksDB(" + dict_ + ")"))
    , dict(dict_)
    , uuid(uuid_)
    , local_base_path(local_base_path_)
    , fallback(fallback_)
{
    initDB();
}

BidirectionalStorageRocksDB::~BidirectionalStorageRocksDB()
{
    try
    {
        closeDB(false);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void BidirectionalStorageRocksDB::clean()
{
    closeDB(true);
    rocksdb.reset(nullptr);
}

void BidirectionalStorageRocksDB::initDB()
{
    rocksdb::DB * db;
    rocksdb::DBOptions db_opts;
    db_opts.create_if_missing = true;
    db_opts.create_missing_column_families = true;
    db_opts.info_log_level = rocksdb::ERROR_LEVEL;

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    rocksdb::ColumnFamilyOptions cf_opts;
    cf_opts.compression_per_level
        = {rocksdb::kNoCompression,
           rocksdb::kNoCompression,
           rocksdb::kLZ4Compression,
           rocksdb::kLZ4Compression,
           rocksdb::kLZ4Compression,
           rocksdb::kZSTD,
           rocksdb::kZSTD};

    column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cf_opts));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("inverse", cf_opts));

    String rocksdb_dir = std::filesystem::path(local_base_path) / (dict + "." + uuid);
    auto status = rocksdb::DB::Open(db_opts, rocksdb_dir, column_families, &handles, &db);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());

    rocksdb = std::unique_ptr<rocksdb::DB>(db);
}

void BidirectionalStorageRocksDB::closeDB(bool clean)
{
    if (rocksdb)
    {
        for (const auto & handle : handles)
        {
            rocksdb->DestroyColumnFamilyHandle(handle);
        }
        rocksdb->Close();

        if (clean)
            std::filesystem::remove_all(std::filesystem::path(local_base_path) / (dict + "." + uuid));
    }
}

std::vector<UInt64> BidirectionalStorageRocksDB::getValues(const std::vector<StringRef> & keys, size_t start, size_t size)
{
    LOG_TRACE(log, "Get {} values from rocksdb", size);

    std::vector<UInt64> ret;
    ret.resize(size);

    std::vector<StringRef> missed_keys;
    std::vector<UInt64> mask;

    std::vector<String> output;
    std::vector<rocksdb::Slice> input;
    input.reserve(size);

    for (auto row : collections::range(size))
    {
        const auto & k = keys[start + row];
        input.emplace_back(rocksdb::Slice(k.data, k.size));
    }

    auto ss = rocksdb->MultiGet(rocksdb::ReadOptions(), {size, handles[0]}, input, &output);
    for (auto row : collections::range(size))
    {
        if (ss[row].ok())
        {
            ret[row] = __builtin_bswap64(*reinterpret_cast<UInt64 *>(output[row].data()));
        }
        else
        {
            missed_keys.emplace_back(keys[start + row]);
            mask.emplace_back(row);
        }
    }

    if (!missed_keys.empty())
    {
        auto fallback_values = fallback->getValues(missed_keys, 0, missed_keys.size());
        rocksdb::WriteBatch batch;
        std::vector<UInt64> encoded_values;
        encoded_values.resize(fallback_values.size());
        for (auto row : collections::range(fallback_values.size()))
        {
            ret[mask[row]] = std::move(fallback_values[row]);
            const auto & k = missed_keys[row];
            const auto & v = ret[mask[row]];
            encoded_values[row] = __builtin_bswap64(v);
            batch.Put(
                handles[0],
                rocksdb::Slice(k.data, k.size),
                rocksdb::Slice(reinterpret_cast<const char *>(&encoded_values[row]), sizeof(UInt64)));
        }

        auto status = rocksdb->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) [[unlikely]]
            LOG_WARNING(log, "Failed to write batch to rocksdb: {}", status.ToString());
    }

    return ret;
}

std::vector<String> BidirectionalStorageRocksDB::getKeys(const std::vector<UInt64> & values, size_t start, size_t size)
{
    LOG_TRACE(log, "Get {} keys from rocksdb", size);

    std::vector<String> ret;
    ret.resize(size);

    std::vector<UInt64> missed_values;
    std::vector<UInt64> mask;

    std::vector<String> output;
    std::vector<rocksdb::Slice> input;
    input.reserve(size);
    std::vector<UInt64> encoded_values;
    encoded_values.resize(size);
    for (auto row : collections::range(size))
    {
        const auto & v = values[start + row];
        encoded_values[row] = __builtin_bswap64(v);
        input.emplace_back(rocksdb::Slice(reinterpret_cast<const char *>(&encoded_values[row]), sizeof(UInt64)));
    }

    auto ss = rocksdb->MultiGet(rocksdb::ReadOptions(), {size, handles[1]}, input, &output);
    for (auto row : collections::range(size))
    {
        if (ss[row].ok())
        {
            ret[row] = std::move(output[row]);
        }
        else
        {
            missed_values.emplace_back(values[start + row]);
            mask.emplace_back(row);
        }
    }

    if (!missed_values.empty())
    {
        auto fallback_keys = fallback->getKeys(missed_values, 0, missed_values.size());
        rocksdb::WriteBatch batch;
        encoded_values.resize(fallback_keys.size());
        for (auto row : collections::range(fallback_keys.size()))
        {
            ret[mask[row]] = std::move(fallback_keys[row]);
            const auto & k = ret[mask[row]];
            const auto & v = missed_values[row];
            encoded_values[row] = __builtin_bswap64(v);
            batch.Put(handles[1], rocksdb::Slice(reinterpret_cast<const char *>(&encoded_values[row]), sizeof(UInt64)), rocksdb::Slice(k));
        }

        auto status = rocksdb->Write(rocksdb::WriteOptions(), &batch);
        if (!status.ok()) [[unlikely]]
            LOG_WARNING(log, "Failed to write batch to rocksdb: {}", status.ToString());
    }

    return ret;
}

BidirectionalStorageRemote::BidirectionalStorageRemote(const String & dict_, const String & target_)
    : log(&Poco::Logger::get("BidirectionalStorageRemote(" + dict_ + ")")), dict(dict_), target(target_)
{
    initClient();
    checkDict();
}

void BidirectionalStorageRemote::initClient()
{
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    auto channel = grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args);
    stub = dictionary::api::Dictionary::NewStub(channel);
}

void BidirectionalStorageRemote::checkDict()
{
    DictMetaReq req;
    DictMetaResp resp;
    req.set_dict(dict);

    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(MAX_PROXY_TIMEOUT_SECONDS));

    auto status = stub->GetDictMeta(&context, req, &resp);
    if (!status.ok()) [[unlikely]]
        throw Exception(
            ErrorCodes::GRPC_ERROR, "Request to dictionary proxy error: {}({})", status.error_message(), status.error_details());

    if (!resp.exists())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Dictionary '{}' not found", dict);
}

std::vector<UInt64> BidirectionalStorageRemote::getValues(const std::vector<StringRef> & keys, size_t start, size_t size)
{
    LOG_TRACE(log, "Get {} values from remote proxy", size);

    std::vector<UInt64> ret;

    size_t cursor = 0;
    while (cursor < size)
    {
        GetValueReq req;
        KeyValueResp resp;

        req.set_dict(dict);
        size_t needs = std::min(MAX_PROXY_BATCH_SIZE, size - cursor);
        req.mutable_keys()->Reserve(needs);
        for (auto row : collections::range(needs))
        {
            const auto & key = keys[start + cursor + row];
            req.mutable_keys()->Add(key.toString());
        }

        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(MAX_PROXY_TIMEOUT_SECONDS));

        auto status = stub->GetValue(&context, req, &resp);
        if (!status.ok()) [[unlikely]]
            throw Exception(
                ErrorCodes::GRPC_ERROR, "Request to dictionary proxy error: {}({})", status.error_message(), status.error_details());

        std::move(resp.values().begin(), resp.values().end(), std::back_inserter(ret));
        cursor += needs;
    }

    return ret;
}

std::vector<String> BidirectionalStorageRemote::getKeys(const std::vector<UInt64> & values, size_t start, size_t size)
{
    LOG_TRACE(log, "Get {} keys from remote proxy", size);

    std::vector<String> ret;

    size_t cursor = 0;
    while (cursor < size)
    {
        GetKeyReq req;
        KeyValueResp resp;

        req.set_dict(dict);
        size_t needs = std::min(MAX_PROXY_BATCH_SIZE, size - cursor);
        req.mutable_values()->Reserve(needs);
        for (auto row : collections::range(needs))
        {
            const auto & value = values[start + cursor + row];
            req.mutable_values()->Add(value);
        }

        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(MAX_PROXY_TIMEOUT_SECONDS));

        auto status = stub->GetKey(&context, req, &resp);
        if (!status.ok()) [[unlikely]]
            throw Exception(
                ErrorCodes::GRPC_ERROR, "Request to dictionary proxy error: {}({})", status.error_message(), status.error_details());

        std::move(resp.keys().begin(), resp.keys().end(), std::back_inserter(ret));
        cursor += needs;
    }

    return ret;
}
}
#endif
