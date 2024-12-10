#pragma once

#include "BidirectionalStorage.h"

#if USE_GRPC
#    include <Core/ExternalResultDescription.h>
#    include <Processors/Sources/SourceWithProgress.h>


namespace DB
{
class BidirectionalSource final : public SourceWithProgress
{
public:
    explicit BidirectionalSource(
        BidirectionalStoragePtr storage_,
        const DB::Block & sample_block_,
        std::vector<StringRef> keys_,
        std::vector<UInt64> values_,
        size_t max_block_size_ = DEFAULT_BLOCK_SIZE);

    ~BidirectionalSource() override = default;

    String getName() const override { return "Bidirectional"; }

private:
    Chunk generate() override;

    BidirectionalStoragePtr storage;
    std::vector<StringRef> keys;
    std::vector<UInt64> values;

    ExternalResultDescription description;

    const size_t max_block_size;
    size_t cursor = 0;
    bool all_read = false;
};
}
#endif
