#include <Columns/ColumnsNumber.h>
#include <DataTypes/Serializations/SerializationArrayOffsets.h>
#include <Common/assert_cast.h>

namespace DB
{

void SerializationArrayOffsets::deserializeBinaryBulkWithMultipleStreams(
    ColumnPtr & column,
    size_t rows_offset,
    size_t limit,
    DeserializeBinaryBulkSettings & settings,
    DeserializeBinaryBulkStatePtr &,
    SubstreamsCache * cache) const
{
    settings.path.push_back(Substream::Regular);

    if (auto cached_column = getFromSubstreamsCache(cache, settings.path))
    {
        if (rows_offset)
            column->assumeMutable()->insertRangeFrom(*cached_column, cached_column->size() - limit, limit);
        else
            column = cached_column;
    }
    else if (ReadBuffer * stream = settings.getter(settings.path))
    {
        auto mutable_column = column->assumeMutable();
        size_t prev_size = mutable_column->size();
        
        /// When rows_offset > 0, we need to read rows_offset + limit rows
        /// because the offsets need to be adjusted relative to the skipped rows
        deserializeBinaryBulk(*mutable_column, *stream, 0, rows_offset + limit, settings.avg_value_size_hint);
        size_t num_read_rows = mutable_column->size() - prev_size;
        size_t actual_new_size = mutable_column->size() - rows_offset;

        /// Always cache the data from current range without applied offsets to be able
        /// to calculate offset for nested data in SerializationArray if the whole array is also read.
        if (cache)
            addToSubstreamsCache(cache, settings.path, mutable_column->cut(prev_size, num_read_rows));

        /// Apply rows_offset if needed.
        if (rows_offset > 0)
        {
            auto & data = assert_cast<ColumnUInt64 &>(*mutable_column).getData();
            for (size_t i = prev_size; i != actual_new_size; ++i)
                data[i] = data[i + rows_offset];
            data.resize(actual_new_size);
        }
    }

    settings.path.pop_back();
}

}

