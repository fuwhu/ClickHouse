#include "Storages/MergeTree/MergeTreeDictionary.h"

#include <DataTypes/DataTypeInterval.h>
#include <IO/HashingWriteBuffer.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <base/TypeLists.h>
#include <base/TypePair.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
}

static constexpr auto DICTIONARY_EXTENSION = ".dict";

IMergeTreeDictionary::IMergeTreeDictionary(const DataTypePtr & data_type_, DictionaryType dict_type_)
    : data_type(data_type_), dict_type(dict_type_), raw_data(data_type->createColumn())
{
}

MergeTreeDictionaryKeyValue::MergeTreeDictionaryKeyValue(const DataTypePtr & data_type_)
    : IMergeTreeDictionary(data_type_, IMergeTreeDictionary::DictionaryType::KEY_VALUE)
{
}

void MergeTreeDictionaryKeyValue::serialize(WriteBuffer & out) const
{
    writeBinary(static_cast<UInt8>(IMergeTreeDictionary::DictionaryType::KEY_VALUE), out);
    writeBinary(static_cast<UInt64>(raw_data->size()), out);

    auto serializer = data_type->getDefaultSerialization();
    serializer->serializeBinaryBulk(*raw_data, out, 0, raw_data->size());
}

void MergeTreeDictionaryKeyValue::deserialize(ReadBuffer & in)
{
    UInt64 dict_size;
    readBinary(dict_size, in);

    auto serializer = data_type->getDefaultSerialization();
    serializer->deserializeBinaryBulk(*raw_data, in, dict_size, 0);

    reverse_index.reserve(dict_size);
    for (auto row : collections::range(dict_size))
        reverse_index.emplace(raw_data->getDataAt(row), row);

    is_dict_built = true;
}

void MergeTreeDictionaryKeyValue::insertDataImpl(StringRef data)
{
    raw_data->insertData(data.data, data.size);
}

void MergeTreeDictionaryKeyValue::insertDataFromImpl(const IColumn & src, size_t n)
{
    raw_data->insertRangeFrom(src, 0, n);
}

void MergeTreeDictionaryKeyValue::buildImpl()
{
    buildFromImpl(*raw_data);
}

void MergeTreeDictionaryKeyValue::buildFromImpl(const IColumn & src)
{
    if (!src.empty())
    {
        IColumn::Permutation perm;
        src.getPermutation(IColumn::PermutationSortDirection::Ascending, IColumn::PermutationSortStability::Stable, 0, 1, perm);
        auto sorted_data = src.permute(perm, 0);

        raw_data = sorted_data->cloneEmpty();
        StringRef last;
        for (auto row : collections::range(sorted_data->size()))
        {
            auto data = sorted_data->getDataAt(row);
            if (row == 0 || last != data)
            {
                raw_data->insertData(data.data, data.size);
                last = data;
            }
        }

        reverse_index.reserve(raw_data->size());
        for (auto row : collections::range(raw_data->size()))
            reverse_index.emplace(raw_data->getDataAt(row), row);
    }

    is_dict_built = true;
}

std::optional<UInt64> MergeTreeDictionaryKeyValue::getIndexImpl(StringRef data) const
{
    auto it = reverse_index.find(data);
    if (it == reverse_index.end())
        return {};

    return it->second;
}

MergeTreeDictionaryStore::MergeTreeDictionaryStore(const IMergeTreeDataPart * part_)
{
    if (part_)
        setMergeTreePart(part_);
}

void MergeTreeDictionaryStore::setMergeTreePart(const IMergeTreeDataPart * part_)
{
    if (part_)
    {
        part = part_;
        part->assertOnDisk();
    }
}

MergeTreeDictionaryPtr MergeTreeDictionaryStore::getDictionary(const String & column_name) const
{
    const auto it = dictionaries.find(column_name);
    if (it != dictionaries.end())
        return it->second;

    return nullptr;
}

void MergeTreeDictionaryStore::addDictionary(const String & column_name, MergeTreeDictionaryPtr dict)
{
    if (!dict)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not add a null dictionary");

    dictionaries.emplace(column_name, std::move(dict));
}

void MergeTreeDictionaryStore::serialize(MergeTreeDataPartChecksums & checksums) const
{
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeDataPart not set");

    if (dictionaries.empty())
        return;

    for (const auto & [column_name, dict] : dictionaries)
    {
        auto file_name = column_name + DICTIONARY_EXTENSION;
        auto out = part->volume->getDisk()->writeFile(part->getFullRelativePath() + file_name);
        HashingWriteBuffer out_hashing(*out);
        dict->serialize(out_hashing);
        checksums.addFile(file_name, out_hashing.count(), out_hashing.getHash());
        out->finalize();
    }
}

void MergeTreeDictionaryStore::deserialize()
{
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeDataPart not set");

    auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
    const auto & sorting_key_columns = metadata_snapshot->getSortingKey().sample_block.getColumnsWithTypeAndName();

    for (const auto & idx : collections::range(sorting_key_columns.size()))
    {
        auto in = part->volume->getDisk()->readFile(part->getFullRelativePath() + sorting_key_columns[idx].name + DICTIONARY_EXTENSION);

        UInt8 dict_type;
        readBinary(dict_type, *in);

        MergeTreeDictionaryMutablePtr dict
            = createDictionary(static_cast<IMergeTreeDictionary::DictionaryType>(dict_type), sorting_key_columns[idx].type);

        dict->deserialize(*in);
        dictionaries.emplace(sorting_key_columns[idx].name, std::move(dict));
    }
}

MergeTreeDictionaryMutablePtr
MergeTreeDictionaryStore::createDictionary(IMergeTreeDictionary::DictionaryType dict_type, const DataTypePtr & data_type)
{
    WhichDataType which(data_type);
    if (which.isString() || which.isFixedString() || which.isDate() || which.isDate32() || which.isDateTime() || which.isUUID()
        || which.isInterval() || which.isInt() || which.isUInt() || which.isFloat())
    {
        if (dict_type == IMergeTreeDictionary::DictionaryType::KEY_VALUE)
        {
            return std::make_shared<MergeTreeDictionaryKeyValue>(data_type);
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported dictionary type");
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unexpected data type for MergeTreeDictionary: {}", data_type->getName());
}

MergeTreeDictionaryPtr MergeTreeDictionaryStore::mergeDictionaries(const std::vector<MergeTreeDictionaryPtr> & dictionaries)
{
    if (dictionaries.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No any MergeTreeDictionary given");

    auto new_dict = createDictionary(dictionaries[0]->dict_type, dictionaries[0]->data_type);

    for (auto i : collections::range(dictionaries.size()))
    {
        if (dictionaries[i]->dict_type != new_dict->dict_type
            || dictionaries[i]->data_type->getTypeId() != new_dict->data_type->getTypeId())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "MergeTreeDictionaries should have the same data type and dictionary type");
        }

        const auto & raw_data = dictionaries[i]->getRawData();
        new_dict->insertDataFrom(raw_data, raw_data.size());
    }

    new_dict->build();

    return new_dict;
}
}
