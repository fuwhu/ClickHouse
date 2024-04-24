#include "BitmapDictionarySource.h"

#include <filesystem>
#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <Dictionaries/DictionarySourceFactory.h>
#include <Dictionaries/DictionarySourceHelpers.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Interpreters/Context.h>
#include <base/logger_useful.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}

static constexpr auto MAX_KEY_LENGTH = 255;

std::string BitmapDictionarySource::toString() const
{
    return "Bitmap: " + configuration.dict_name;
}

BitmapDictionarySource::BitmapDictionarySource(
    const DictionaryStructure & dict_struct_, const Configuration & configuration_, const Block & sample_block_, ContextPtr context_)
    : log(&Poco::Logger::get("BitmapDictionarySource"))
    , dict_struct(dict_struct_)
    , configuration(configuration_)
    , sample_block(sample_block_)
    , context(context_)
{
    if (configuration.dict_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bitmap dictionary name can not be empty");

    initStorage();
}

BitmapDictionarySource::BitmapDictionarySource(const BitmapDictionarySource & other)
    : log(&Poco::Logger::get("BitmapDictionarySource"))
    , dict_struct(other.dict_struct)
    , configuration(other.configuration)
    , sample_block(other.sample_block)
    , context(Context::createCopy(other.context))
{
    initStorage();
}

void BitmapDictionarySource::initStorage()
{
    auto remote_dict_proxy = std::make_shared<BitmapDictionaryStorageRemote>(configuration.dict_name, configuration.dict_proxy_address);
    storage = std::make_shared<BitmapDictionaryStorageRocksDB>(
        configuration.dict_name + "-" + configuration.uuid,
        configuration.dict_rocksdb_path,
        context->getRocksDBLRUCache(),
        remote_dict_proxy);
}

Pipe BitmapDictionarySource::loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows)
{
    if (key_columns.size() != dict_struct.key->size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The size of key_columns does not equal to the size of dictionary key");

    LOG_TRACE(log, "loadKeys {}, size = {}", toString(), requested_rows.size());

    /// keys -> values
    if (key_columns[1]->isNullAt(0))
    {
        std::vector<StringRef> keys;
        keys.resize(requested_rows.size());
        for (auto row : requested_rows)
        {
            if (key_columns[0]->isNullAt(row)) [[unlikely]]
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null key has no mapping");

            if (key_columns[0]->getDataAt(row).size > MAX_KEY_LENGTH) [[unlikely]]
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Key '{}' is too long, max length: {}",
                    key_columns[0]->getDataAt(row).toString(),
                    MAX_KEY_LENGTH);

            keys[row] = key_columns[0]->getDataAt(row);
        }
        return Pipe(std::make_shared<BitmapSource>(storage, sample_block, std::move(keys), std::vector<UInt64>()));
    }
    /// values -> keys
    else if (key_columns[0]->isNullAt(0))
    {
        std::vector<UInt64> values;
        values.resize(requested_rows.size());
        for (auto row : requested_rows)
        {
            if (key_columns[1]->isNullAt(row)) [[unlikely]]
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Null value has no mapping");

            values[row] = key_columns[1]->get64(row);
        }
        return Pipe(std::make_shared<BitmapSource>(storage, sample_block, std::vector<StringRef>(), std::move(values)));
    }

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Use BitmapDictionary as dictGet('dict_name', 'value', (column, null)) to get value"
        "or dictGet('dict_name', 'key', (null, column)) to get key");
}

void BitmapDictionarySource::clean()
{
    storage->clean();
}

void registerDictionarySourceBitmap(DictionarySourceFactory & factory)
{
    auto create_dict_source = [=](const DictionaryStructure & dict_struct,
                                  const Poco::Util::AbstractConfiguration & config,
                                  const String & config_prefix,
                                  Block & sample_block,
                                  ContextPtr global_context,
                                  const std::string & /* default_database */,
                                  bool /* created_from_ddl */) -> DictionarySourcePtr
    {
        // Check layout
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("dictionary.layout", keys);
        if (keys.size() != 1)
            throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Element dictionary.layout should have exactly one child element");
        if (keys.front() != "complex_key_direct")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Bitmap dictionary layout must always be complex_key_direct, but got {}", keys.front());

        auto check_support_datatype = [=](const DataTypePtr type) -> bool
        {
            WhichDataType which(type);
            // Only support String type now
            return
                // which.isInt() || which.isUInt() || which.isDecimal() || which.isFloat() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64() || which.isUUID() ||
                which.isStringOrFixedString();
        };

        if (!dict_struct.key || (*dict_struct.key).size() != 2 || dict_struct.attributes.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad definition for bitmap dictionary, should have 2 keys and 2 attributes");

        if (dict_struct.attributes[0].name != "key" || dict_struct.attributes[1].name != "value")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad definition for bitmap dictionary, attributes name must be ('key', 'value')");

        if (!check_support_datatype((dict_struct.attributes)[0].type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Bad definition for bitmap dictionary, unsupported data type: {}",
                (*dict_struct.key)[0].type->getName());

        if (!(*dict_struct.key)[0].type->isNullable() || !(*dict_struct.key)[1].type->isNullable())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad definition for bitmap dictionary, the keys should be nullable");

        if (removeNullable((*dict_struct.key)[0].type)->getTypeId() != (dict_struct.attributes)[0].type->getTypeId())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Bad definition for bitmap dictionary, the 1st key must be Nullable({})",
                (dict_struct.attributes)[0].type->getName());

        if (!isUInt64(removeNullable((*dict_struct.key)[1].type)) || !isUInt64(dict_struct.attributes[1].type))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Bad definition for bitmap dictionary, the 2nd key must be Nullable(UInt64) and the 2nd attribute must be UInt64");

        auto bitmap_config_prfix = config_prefix + ".bitmap";
        auto dict = config.getString(bitmap_config_prfix + ".dict", "");
        auto uuid = config.getString("dictionary.uuid", "");

        const auto & global_conf = global_context->getConfigRef();
        auto proxy_addr = global_conf.getString("dictionary_proxy");

        String default_path = std::filesystem::path(global_conf.getString("path")) / "dictionary";
        auto rocksdb_path = global_conf.getString("dictionary_local_path", default_path);
        std::filesystem::create_directories(rocksdb_path);

        BitmapDictionarySource::Configuration configuration{
            .uuid = uuid, .dict_name = dict, .dict_proxy_address = proxy_addr, .dict_rocksdb_path = rocksdb_path};
        return std::make_shared<BitmapDictionarySource>(dict_struct, configuration, sample_block, global_context);
    };

    factory.registerSource("bitmap", create_dict_source);
}
}
