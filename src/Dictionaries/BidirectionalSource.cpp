#include "BidirectionalSource.h"

#if USE_GRPC
#    include <Columns/ColumnNullable.h>
#    include <Columns/ColumnString.h>
#    include <Columns/ColumnVector.h>
#    include <base/range.h>

namespace DB
{
BidirectionalSource::BidirectionalSource(
    BidirectionalStoragePtr storage_,
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

Chunk BidirectionalSource::generate()
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
        assert(output.size() == needs);

        size_t chars_t_size = 0;
        for (auto offset : collections::range(needs))
        {
            const auto & k = keys[cursor + offset];
            chars_t_size += k.size;
        }

        auto & column_chars_t = assert_cast<ColumnString &>(*columns[2]).getChars();
        auto & column_offsets = assert_cast<ColumnString &>(*columns[2]).getOffsets();
        auto & column_data = assert_cast<ColumnVector<UInt64> &>(*columns[3]).getData();

        column_chars_t.reserve_exact(chars_t_size);
        column_offsets.reserve_exact(needs);
        column_data.reserve_exact(needs);

        for (auto offset : collections::range(needs))
        {
            const auto & k = keys[cursor + offset];
            const auto & v = output[offset];

            column_chars_t.insert_assume_reserved(k.data, k.data + k.size);
            column_chars_t.emplace_back(0);
            column_offsets.emplace_back(column_chars_t.size());
            column_data.emplace_back(v);
        }

        assert_cast<ColumnNullable &>(*columns[0]).insertRangeFromNotNullable(*columns[2], 0, needs);
        columns[1]->insertManyDefaults(needs);
    }
    else if (!values.empty())
    {
        needs = std::min(max_block_size, values.size() - cursor);

        std::vector<String> output = storage->getKeys(values, cursor, needs);
        assert(output.size() == needs);

        size_t chars_t_size = 0;
        for (auto offset : collections::range(needs))
        {
            const auto & k = output[offset];
            chars_t_size += k.size();
        }

        auto & column_chars_t = assert_cast<ColumnString &>(*columns[2]).getChars();
        auto & column_offsets = assert_cast<ColumnString &>(*columns[2]).getOffsets();
        auto & column_data = assert_cast<ColumnVector<UInt64> &>(*columns[3]).getData();

        column_chars_t.reserve_exact(chars_t_size);
        column_offsets.reserve_exact(needs);
        column_data.reserve_exact(needs);

        for (auto offset : collections::range(needs))
        {
            const auto & k = output[offset];
            const auto & v = values[cursor + offset];

            column_chars_t.insert_assume_reserved(k.data(), k.data() + k.size());
            column_chars_t.emplace_back(0);
            column_offsets.emplace_back(column_chars_t.size());
            column_data.emplace_back(v);
        }

        columns[0]->insertManyDefaults(needs);
        assert_cast<ColumnNullable &>(*columns[1]).insertRangeFromNotNullable(*columns[3], 0, needs);
    }

    cursor += needs;

    return Chunk(std::move(columns), needs);
}
}
#endif
