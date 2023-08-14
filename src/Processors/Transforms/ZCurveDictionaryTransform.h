#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

class MergeTreeDictionaryStore;

class ZCurveDictionaryTransform final : public ISimpleTransform
{
public:
    ZCurveDictionaryTransform(
        const Block & header_,
        const std::vector<String> & z_curve_columns_,
        const MergeTreeDictionaryStore & dict_store_,
        ContextPtr context_);

    String getName() const override { return "ZCurveDictionaryTransform"; }

protected:
    void transform(Chunk & chunk) override;

    static Block transformHeader(const Block & header);

private:
    std::vector<String> z_curve_columns;
    const MergeTreeDictionaryStore & dict_store;
    ContextPtr context;
};
}
