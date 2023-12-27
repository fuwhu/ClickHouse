#pragma once

#include <Columns/ReverseIndex.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{
class MergeTreeData;
class IMergeTreeDataPart;
class MergeTreeRowMappingStore;
struct MergeTreeDataPartChecksums;

class IMergeTreeRowMapping
{
public:
    enum class MappingType : UInt8
    {
        KEY_VALUE,
        UNKNOWN = 255
    };

    explicit IMergeTreeRowMapping(const DataTypePtr & data_type_, MappingType mapping_type_);

    virtual ~IMergeTreeRowMapping() = default;

    void insertData(StringRef data)
    {
        if (built)
            return;

        insertDataImpl(data);
    }

    void insertDataFrom(const IColumn & src, size_t n)
    {
        if (built)
            return;

        insertDataFromImpl(src, n);
    }

    void build()
    {
        if (built)
            return;

        buildImpl();
    }

    void buildFrom(const IColumn & src)
    {
        if (built)
            return;

        buildFromImpl(src);
    }

    virtual std::optional<UInt64> getIndex(StringRef data) const
    {
        if (!built)
            return {};

        return getIndexImpl(data);
    }

    const IColumn & getRawData() const { return *raw_data; }

protected:
    friend class MergeTreeRowMappingStore;

    virtual void insertDataImpl(StringRef data) = 0;

    virtual void insertDataFromImpl(const IColumn & src, size_t n) = 0;

    virtual void buildFromImpl(const IColumn & src) = 0;

    virtual std::optional<UInt64> getIndexImpl(StringRef data) const = 0;

    virtual void buildImpl() = 0;

    virtual void serialize(WriteBuffer & out) const = 0;

    virtual void deserialize(ReadBuffer & in) = 0;

    DataTypePtr data_type;
    MappingType mapping_type{MappingType::UNKNOWN};
    IColumn::MutablePtr raw_data;
    bool built{false};
};

using MergeTreeRowMappingPtr = std::shared_ptr<const IMergeTreeRowMapping>;
using MergeTreeRowMappingMutablePtr = std::shared_ptr<IMergeTreeRowMapping>;
using MergeTreeRowMappingStorePtr = std::unique_ptr<MergeTreeRowMappingStore>;

class MergeTreeRowMappingKeyValue final : public IMergeTreeRowMapping
{
public:
    explicit MergeTreeRowMappingKeyValue(const DataTypePtr & data_type_);

    MergeTreeRowMappingKeyValue(const MergeTreeRowMappingKeyValue &) = delete;

    void insertDataImpl(StringRef data) override;

    void insertDataFromImpl(const IColumn & src, size_t n) override;

    void buildFromImpl(const IColumn & src) override;

    std::optional<UInt64> getIndexImpl(StringRef data) const override;

    void buildImpl() override;

protected:
    void serialize(WriteBuffer & out) const override;

    void deserialize(ReadBuffer & in) override;

private:
    absl::flat_hash_map<StringRef, UInt64, StringRefHash> reverse_index;
};

class MergeTreeRowMappingStore
{
public:
    static constexpr auto MAPPING_FILE_EXTENSION = ".mapping";

    explicit MergeTreeRowMappingStore(const IMergeTreeDataPart * part_ = nullptr);

    void setMergeTreePart(const IMergeTreeDataPart * part_);

    MergeTreeRowMappingPtr getMapping(const String & column_name) const;

    void addMapping(const String & column_name, MergeTreeRowMappingPtr mapping);

    void serialize(MergeTreeDataPartChecksums & checksums) const;

    void deserialize();

    static MergeTreeRowMappingMutablePtr createMapping(IMergeTreeRowMapping::MappingType mapping_type, const DataTypePtr & data_type);

    static MergeTreeRowMappingPtr mergeMappings(const std::vector<MergeTreeRowMappingPtr> & mappings);

private:
    std::unordered_map<String, MergeTreeRowMappingPtr> column_mappings;
    const IMergeTreeDataPart * part;
};
}
