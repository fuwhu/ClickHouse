#pragma once

#include "BitmapDictionaryStorage.h"

#include <Core/Block.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionarySource.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class BitmapDictionarySource final : public IDictionarySource
{
public:
    struct Configuration
    {
        const String uuid;
        const String dict_name;
        const String dict_proxy_address;
        const String dict_rocksdb_path;
    };

    BitmapDictionarySource(
        const DictionaryStructure & dict_struct_, const Configuration & configuration_, const Block & sample_block_, ContextPtr context_);

    BitmapDictionarySource(const BitmapDictionarySource & other);

    BitmapDictionarySource & operator=(const BitmapDictionarySource &) = delete;

    ~BitmapDictionarySource() override = default;

    void initStorage();

    Pipe loadAll() override { throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadAll is unsupported for BitmapDictionarySource"); }

    Pipe loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for BitmapDictionarySource");
    }

    bool supportsSelectiveLoad() const override { return true; }

    Pipe loadIds(const std::vector<UInt64> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadIds is unsupported for BitmapDictionarySource");
    }

    Pipe loadKeys(const Columns & key_columns, const std::vector<size_t> & requested_rows) override;

    bool isModified() const override { return true; }

    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<BitmapDictionarySource>(*this); }

    std::string toString() const override;

    void clean() override;

private:
    Poco::Logger * log;

    const DictionaryStructure dict_struct;
    const Configuration configuration;

    BitmapDictionaryStoragePtr storage;
    Block sample_block;
    ContextPtr context;
};
}
