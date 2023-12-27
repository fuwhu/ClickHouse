#include "Storages/MergeTree/MergeTreeRowMapping.h"

#include <Compression/CompressedWriteBuffer.h>
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

IMergeTreeRowMapping::IMergeTreeRowMapping(const DataTypePtr & data_type_, MappingType mapping_type_)
    : data_type(data_type_), mapping_type(mapping_type_), raw_data(data_type->createColumn())
{
}

MergeTreeRowMappingKeyValue::MergeTreeRowMappingKeyValue(const DataTypePtr & data_type_)
    : IMergeTreeRowMapping(data_type_, IMergeTreeRowMapping::MappingType::KEY_VALUE)
{
}

void MergeTreeRowMappingKeyValue::serialize(WriteBuffer & out) const
{
    writeBinary(static_cast<UInt8>(IMergeTreeRowMapping::MappingType::KEY_VALUE), out);
    writeBinary(static_cast<UInt64>(raw_data->size()), out);

    auto serializer = data_type->getDefaultSerialization();
    serializer->serializeBinaryBulk(*raw_data, out, 0, raw_data->size());
}

void MergeTreeRowMappingKeyValue::deserialize(ReadBuffer & in)
{
    UInt64 mapping_size;
    readBinary(mapping_size, in);

    auto serializer = data_type->getDefaultSerialization();
    serializer->deserializeBinaryBulk(*raw_data, in, mapping_size, 0);

    reverse_index.reserve(mapping_size);
    for (auto row : collections::range(mapping_size))
        reverse_index.emplace(raw_data->getDataAt(row), row);

    built = true;
}

void MergeTreeRowMappingKeyValue::insertDataImpl(StringRef data)
{
    raw_data->insertData(data.data, data.size);
}

void MergeTreeRowMappingKeyValue::insertDataFromImpl(const IColumn & src, size_t n)
{
    raw_data->insertRangeFrom(src, 0, n);
}

void MergeTreeRowMappingKeyValue::buildImpl()
{
    buildFromImpl(*raw_data);
}

void MergeTreeRowMappingKeyValue::buildFromImpl(const IColumn & src)
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

    built = true;
}

std::optional<UInt64> MergeTreeRowMappingKeyValue::getIndexImpl(StringRef data) const
{
    auto it = reverse_index.find(data);
    if (it == reverse_index.end())
        return {};

    return it->second;
}

MergeTreeRowMappingStore::MergeTreeRowMappingStore(const IMergeTreeDataPart * part_)
{
    if (part_)
        setMergeTreePart(part_);
}

void MergeTreeRowMappingStore::setMergeTreePart(const IMergeTreeDataPart * part_)
{
    if (part_)
    {
        part = part_;
        part->assertOnDisk();
    }
}

MergeTreeRowMappingPtr MergeTreeRowMappingStore::getMapping(const String & column_name) const
{
    const auto it = column_mappings.find(column_name);
    if (it != column_mappings.end())
        return it->second;

    return nullptr;
}

void MergeTreeRowMappingStore::addMapping(const String & column_name, MergeTreeRowMappingPtr mapping)
{
    if (!mapping)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Can not add a null mapping");

    column_mappings.emplace(column_name, std::move(mapping));
}

void MergeTreeRowMappingStore::serialize(MergeTreeDataPartChecksums & checksums) const
{
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeDataPart not set");

    if (column_mappings.empty())
        return;

    for (const auto & [column_name, mapping] : column_mappings)
    {
        auto file_name = column_name + MAPPING_FILE_EXTENSION;
        auto out = part->volume->getDisk()->writeFile(part->getFullRelativePath() + file_name);
        HashingWriteBuffer out_hashing(*out);
        mapping->serialize(out_hashing);

        out_hashing.next();
        checksums.addFile(file_name, out_hashing.count(), out_hashing.getHash());
        out->finalize();
        out->sync();
    }
}

void MergeTreeRowMappingStore::deserialize()
{
    if (!part)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "MergeTreeDataPart not set");

    auto metadata_snapshot = part->storage.getInMemoryMetadataPtr();
    const auto & sorting_key_columns = metadata_snapshot->getSortingKey().sample_block.getColumnsWithTypeAndName();

    for (const auto & idx : collections::range(sorting_key_columns.size()))
    {
        auto in = part->volume->getDisk()->readFile(part->getFullRelativePath() + sorting_key_columns[idx].name + MAPPING_FILE_EXTENSION);

        UInt8 mapping_type;
        readBinary(mapping_type, *in);

        MergeTreeRowMappingMutablePtr mapping
            = createMapping(static_cast<IMergeTreeRowMapping::MappingType>(mapping_type), sorting_key_columns[idx].type);

        mapping->deserialize(*in);
        column_mappings.emplace(sorting_key_columns[idx].name, std::move(mapping));
    }
}

MergeTreeRowMappingMutablePtr
MergeTreeRowMappingStore::createMapping(IMergeTreeRowMapping::MappingType mapping_type, const DataTypePtr & data_type)
{
    WhichDataType which(data_type);
    if (which.isString() || which.isFixedString() || which.isDate() || which.isDate32() || which.isDateTime() || which.isUUID()
        || which.isInterval() || which.isInt() || which.isUInt() || which.isFloat())
    {
        if (mapping_type == IMergeTreeRowMapping::MappingType::KEY_VALUE)
        {
            return std::make_shared<MergeTreeRowMappingKeyValue>(data_type);
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unsupported mapping type");
    }

    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Unexpected data type for MergeTreeRowMapping: {}", data_type->getName());
}

MergeTreeRowMappingPtr MergeTreeRowMappingStore::mergeMappings(const std::vector<MergeTreeRowMappingPtr> & mappings)
{
    if (mappings.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No any MergeTreeRowMapping given");

    auto new_mapping = createMapping(mappings[0]->mapping_type, mappings[0]->data_type);

    for (auto i : collections::range(mappings.size()))
    {
        if (mappings[i]->mapping_type != new_mapping->mapping_type
            || mappings[i]->data_type->getTypeId() != new_mapping->data_type->getTypeId())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "All mappings should have same mapping type and data type");
        }

        const auto & raw_data = mappings[i]->getRawData();
        new_mapping->insertDataFrom(raw_data, raw_data.size());
    }

    new_mapping->build();

    return new_mapping;
}
}
