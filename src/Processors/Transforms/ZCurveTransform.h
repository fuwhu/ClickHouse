#pragma once

#include <Interpreters/Context_fwd.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

class MergeTreeRowMappingStore;

class ZCurveTransform final : public ISimpleTransform
{
public:
    ZCurveTransform(
        const Block & header_,
        const std::vector<String> & z_curve_columns_,
        const MergeTreeRowMappingStore & mapping_store_,
        ContextPtr context_);

    String getName() const override { return "ZCurveDictionaryTransform"; }

protected:
    void transform(Chunk & chunk) override;

    static Block transformHeader(const Block & header);

private:
    std::vector<String> z_curve_columns;
    const MergeTreeRowMappingStore & mapping_store;
    ContextPtr context;
};
}
