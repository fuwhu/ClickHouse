#pragma once

#include <Columns/ReverseIndex.h>
#include <absl/container/flat_hash_map.h>

namespace DB
{
class MergeTreeData;
class IMergeTreeDataPart;
class MergeTreeDictionaryStore;
struct MergeTreeDataPartChecksums;

class IMergeTreeDictionary
{
public:
    enum class DictionaryType : UInt8
    {
        KEY_VALUE,
        UNKNOWN = 255
    };

    explicit IMergeTreeDictionary(const DataTypePtr & data_type_, DictionaryType dict_type_);

    virtual ~IMergeTreeDictionary() = default;

    void insertData(StringRef data)
    {
        if (is_dict_built)
            return;

        insertDataImpl(data);
    }

    void insertDataFrom(const IColumn & src, size_t n)
    {
        if (is_dict_built)
            return;

        insertDataFromImpl(src, n);
    }

    void build()
    {
        if (is_dict_built)
            return;

        buildImpl();
    }

    void buildFrom(const IColumn & src)
    {
        if (is_dict_built)
            return;

        buildFromImpl(src);
    }

    virtual std::optional<UInt64> getIndex(StringRef data) const
    {
        if (!is_dict_built)
            return {};

        return getIndexImpl(data);
    }

    const IColumn & getRawData() const { return *raw_data; }

protected:
    friend class MergeTreeDictionaryStore;

    virtual void insertDataImpl(StringRef data) = 0;

    virtual void insertDataFromImpl(const IColumn & src, size_t n) = 0;

    virtual void buildFromImpl(const IColumn & src) = 0;

    virtual std::optional<UInt64> getIndexImpl(StringRef data) const = 0;

    virtual void buildImpl() = 0;

    virtual void serialize(WriteBuffer & out) const = 0;

    virtual void deserialize(ReadBuffer & in) = 0;

    DataTypePtr data_type;
    DictionaryType dict_type{DictionaryType::UNKNOWN};
    IColumn::MutablePtr raw_data;
    bool is_dict_built{false};
};

using MergeTreeDictionaryPtr = std::shared_ptr<const IMergeTreeDictionary>;
using MergeTreeDictionaryMutablePtr = std::shared_ptr<IMergeTreeDictionary>;
using MergeTreeDictionaryStorePtr = std::unique_ptr<MergeTreeDictionaryStore>;

class MergeTreeDictionaryKeyValue final : public IMergeTreeDictionary
{
public:
    explicit MergeTreeDictionaryKeyValue(const DataTypePtr & data_type_);

    MergeTreeDictionaryKeyValue(const MergeTreeDictionaryKeyValue &) = delete;

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

class MergeTreeDictionaryStore
{
public:
    explicit MergeTreeDictionaryStore(const IMergeTreeDataPart * part_ = nullptr);

    void setMergeTreePart(const IMergeTreeDataPart * part_);

    MergeTreeDictionaryPtr getDictionary(const String & column_name) const;

    void addDictionary(const String & column_name, MergeTreeDictionaryPtr dict);

    void serialize(MergeTreeDataPartChecksums & checksums) const;

    void deserialize();

    static MergeTreeDictionaryMutablePtr createDictionary(IMergeTreeDictionary::DictionaryType dict_type, const DataTypePtr & data_type);

    static MergeTreeDictionaryPtr mergeDictionaries(const std::vector<MergeTreeDictionaryPtr> & dictionaries);

private:
    std::unordered_map<String, MergeTreeDictionaryPtr> dictionaries;
    const IMergeTreeDataPart * part;
};
}
