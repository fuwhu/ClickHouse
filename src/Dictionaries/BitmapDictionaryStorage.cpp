#include "BitmapDictionaryStorage.h"

#include <filesystem>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <IO/ReadHelpers.h>
#include <base/logger_useful.h>
#include <base/range.h>
#include <rocksdb/table.h>

using dictionary::api::GetKeyReq;
using dictionary::api::GetValueReq;
using dictionary::api::KeyValueResp;


namespace DB
{
namespace ErrorCodes
{
    extern const int GRPC_ERROR;
    extern const int ROCKSDB_ERROR;
}

static constexpr size_t MAX_PROXY_BATCH_SIZE = 1000;
static constexpr size_t MAX_PROXY_TIMEOUT_SECONDS = 5;

BitmapDictionaryStorageRocksDB::BitmapDictionaryStorageRocksDB(
    const String & dict_,
    const String & local_path_,
    const std::shared_ptr<rocksdb::Cache> & lru_cache_,
    BitmapDictionaryStoragePtr fallback_)
    : log(&Poco::Logger::get("BitmapDictionaryStorageRocksDB(" + dict_ + ")")), dict(dict_), local_path(local_path_), fallback(fallback_)
{
    initDB(lru_cache_);
}

BitmapDictionaryStorageRocksDB::~BitmapDictionaryStorageRocksDB()
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

void BitmapDictionaryStorageRocksDB::clean()
{
    closeDB(true);
    rocksdb.reset(nullptr);
}

void BitmapDictionaryStorageRocksDB::initDB(const std::shared_ptr<rocksdb::Cache> & lru_cache)
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

    if (lru_cache)
    {
        rocksdb::BlockBasedTableOptions table_opts;
        table_opts.block_cache = lru_cache;
        cf_opts.table_factory.reset(NewBlockBasedTableFactory(table_opts));
    }

    column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, cf_opts));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("inverse", cf_opts));

    String rocksdb_dir = std::filesystem::path(local_path) / dict;
    auto status = rocksdb::DB::Open(db_opts, rocksdb_dir, column_families, &handles, &db);
    if (!status.ok())
        throw Exception(ErrorCodes::ROCKSDB_ERROR, "Fail to open rocksdb path at: {}: {}", rocksdb_dir, status.ToString());

    rocksdb = std::unique_ptr<rocksdb::DB>(db);
}

void BitmapDictionaryStorageRocksDB::closeDB(bool clean)
{
    if (rocksdb)
    {
        for (const auto & handle : handles)
        {
            rocksdb->DestroyColumnFamilyHandle(handle);
        }
        rocksdb->Close();

        if (clean)
            std::filesystem::remove_all(std::filesystem::path(local_path) / dict);
    }
}

std::vector<UInt64> BitmapDictionaryStorageRocksDB::getValues(const std::vector<StringRef> & keys, size_t start, size_t size)
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

    if (fallback && !missed_keys.empty())
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

std::vector<String> BitmapDictionaryStorageRocksDB::getKeys(const std::vector<UInt64> & values, size_t start, size_t size)
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

    if (fallback && !missed_values.empty())
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

BitmapDictionaryStorageRemote::BitmapDictionaryStorageRemote(const String & dict_, const String & target_)
    : log(&Poco::Logger::get("BitmapDictionaryStorageRemote(" + dict_ + ")")), dict(dict_), target(target_)
{
    initClient();
}

void BitmapDictionaryStorageRemote::initClient()
{
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_USE_LOCAL_SUBCHANNEL_POOL, 1);
    auto channel = grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args);
    stub = dictionary::api::Dictionary::NewStub(channel);
}

std::vector<UInt64> BitmapDictionaryStorageRemote::getValues(const std::vector<StringRef> & keys, size_t start, size_t size)
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
            throw Exception(ErrorCodes::GRPC_ERROR, "Request to dictionary proxy error: {}", status.error_details());

        std::move(resp.values().begin(), resp.values().end(), std::back_inserter(ret));
        cursor += needs;
    }

    return ret;
}

std::vector<String> BitmapDictionaryStorageRemote::getKeys(const std::vector<UInt64> & values, size_t start, size_t size)
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
            throw Exception(ErrorCodes::GRPC_ERROR, "Request to dictionary proxy error: {}", status.error_details());

        std::move(resp.keys().begin(), resp.keys().end(), std::back_inserter(ret));
        cursor += needs;
    }

    return ret;
}

BitmapSource::BitmapSource(
    BitmapDictionaryStoragePtr storage_,
    const DB::Block & sample_block_,
    std::vector<StringRef> keys_,
    std::vector<UInt64> values_,
    size_t max_block_size_)
    : SourceWithProgress(sample_block_)
    , storage(storage_)
    , keys(std::move(keys_))
    , values(std::move(values_))
    , max_block_size(max_block_size_)
{
    description.init(sample_block_);
}

Chunk BitmapSource::generate()
{
    if (description.sample_block.rows() == 0 || (cursor >= keys.size() && values.empty()) || (cursor >= values.size() && keys.empty()))
    {
        all_read = true;
    }

    if (all_read)
        return {};

    const size_t size = description.sample_block.columns();
    MutableColumns columns(size);

    for (size_t i = 0; i < size; ++i)
        columns[i] = description.sample_block.getByPosition(i).column->cloneEmpty();

    size_t needs = 0;
    if (!keys.empty())
    {
        needs = std::min(max_block_size, keys.size() - cursor);

        std::vector<UInt64> output = storage->getValues(keys, cursor, needs);
        for (auto row : collections::range(output.size()))
        {
            const auto & k = keys[cursor + row];
            const auto & v = output[row];

            columns[2]->insertData(k.data, k.size);
            assert_cast<ColumnVector<UInt64> &>(*columns[3]).insertValue(v);
        }

        assert_cast<ColumnNullable &>(*columns[0]).insertRangeFromNotNullable(*columns[2], 0, output.size());
        columns[1]->insertManyDefaults(output.size());
    }
    else if (!values.empty())
    {
        needs = std::min(max_block_size, values.size() - cursor);

        std::vector<String> output = storage->getKeys(values, cursor, needs);
        for (auto row : collections::range(output.size()))
        {
            const auto & k = output[row];
            const auto & v = values[cursor + row];

            columns[2]->insertData(k.data(), k.size());
            assert_cast<ColumnVector<UInt64> &>(*columns[3]).insertValue(v);
        }

        columns[0]->insertManyDefaults(output.size());
        assert_cast<ColumnNullable &>(*columns[1]).insertRangeFromNotNullable(*columns[3], 0, output.size());
    }

    cursor += needs;

    return Chunk(std::move(columns), needs);
}
}
