#include <AggregateFunctions/AggregateFunctionBsiCommon.h>

namespace DB
{

AggregateFunctionBsiAggData::AggregateFunctionBsiAggData() : ids_with_bsi(65)
{
}

void AggregateFunctionBsiAggData::add(
    const ColumnAggregateFunction::Container & rhs_data, const size_t & rhs_start, const size_t & rhs_size, bool only_merge)
{
    auto & ids = ids_with_bsi[0];
    BitmapDataType & rhs_ids = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start]);

    if (!ids.roaring_bitmap_with_small_set.rb_and_cardinality(rhs_ids.roaring_bitmap_with_small_set) || only_merge)
        doMergeOperation(rhs_data, rhs_start, rhs_size);
    else
    {
        ids.roaring_bitmap_with_small_set.rb_or(rhs_ids.roaring_bitmap_with_small_set);
        int slice_number = max_slice_number;
        int rhs_slice_number = -1;

        if (slice_number == -1)
            slice_number = getValidSliceNumber(ids_with_bsi);

        rhs_slice_number = getValidSliceNumberFromData(rhs_data, rhs_start, rhs_size);

        size_t new_slice_number = std::max(slice_number, rhs_slice_number) + 1;
        BitmapDataType bitmap_carry_out;

        for (int i = 1; i <= std::min(slice_number, rhs_slice_number); ++i)
        {
            BitmapDataType & rhs_bsi_slice = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start + i]);
            mergeCommonSlice(ids_with_bsi[i], rhs_bsi_slice, bitmap_carry_out);
        }

        if (slice_number > rhs_slice_number)
        {
            for (int i = rhs_slice_number + 1; i <= slice_number; ++i)
                mergeSurplusSlice(ids_with_bsi[i], bitmap_carry_out);
        }
        else
        {
            for (int i = slice_number + 1; i <= rhs_slice_number; ++i)
            {
                BitmapDataType & rhs_bsi_slice = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start + i]);
                mergeSurplusSlice(ids_with_bsi[i], rhs_bsi_slice, bitmap_carry_out);
            }
        }

        if (bitmap_carry_out.roaring_bitmap_with_small_set.size())
        {
            ids_with_bsi[new_slice_number].roaring_bitmap_with_small_set.rb_or(bitmap_carry_out.roaring_bitmap_with_small_set);
            max_slice_number = static_cast<int>(new_slice_number);
        }
        else
            max_slice_number = static_cast<int>(new_slice_number) - 1;
    }
}

void AggregateFunctionBsiAggData::add(const BSIDataType & rhs_ids_with_bsi, bool only_merge)
{
    auto & ids = ids_with_bsi[0];
    const auto & rhs_ids = rhs_ids_with_bsi[0];

    if (!ids.roaring_bitmap_with_small_set.rb_and_cardinality(rhs_ids.roaring_bitmap_with_small_set) || only_merge)
        doMergeOperation(rhs_ids_with_bsi);
    else
    {
        ids.roaring_bitmap_with_small_set.rb_or(rhs_ids.roaring_bitmap_with_small_set);
        int slice_number = max_slice_number;
        int rhs_slice_number = -1;

        if (slice_number == -1)
            slice_number = getValidSliceNumber(ids_with_bsi);

        rhs_slice_number = getValidSliceNumber(rhs_ids_with_bsi);

        size_t new_slice_number = std::max(slice_number, rhs_slice_number) + 1;
        BitmapDataType bitmap_carry_out;

        for (int i = 1; i <= std::min(slice_number, rhs_slice_number); ++i)
            mergeCommonSlice(ids_with_bsi[i], rhs_ids_with_bsi[i], bitmap_carry_out);

        if (slice_number > rhs_slice_number)
        {
            for (int i = rhs_slice_number + 1; i <= slice_number; ++i)
                mergeSurplusSlice(ids_with_bsi[i], bitmap_carry_out);
        }
        else
        {
            for (int i = slice_number + 1; i <= rhs_slice_number; ++i)
                mergeSurplusSlice(ids_with_bsi[i], rhs_ids_with_bsi[i], bitmap_carry_out);
        }

        if (bitmap_carry_out.roaring_bitmap_with_small_set.size())
        {
            ids_with_bsi[new_slice_number].roaring_bitmap_with_small_set.rb_or(bitmap_carry_out.roaring_bitmap_with_small_set);
            max_slice_number = static_cast<int>(new_slice_number);
        }
        else
            max_slice_number = static_cast<int>(new_slice_number) - 1;
    }
}

void AggregateFunctionBsiAggData::doMergeOperation(
    const ColumnAggregateFunction::Container & rhs_data, const size_t & rhs_start, const size_t & rhs_size)
{
    auto & ids = ids_with_bsi[0];
    BitmapDataType & rhs_ids = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start]);

    ids.roaring_bitmap_with_small_set.rb_or(rhs_ids.roaring_bitmap_with_small_set);
    int slice_number = max_slice_number;
    int rhs_slice_number = -1;

    if (slice_number == -1)
        slice_number = getValidSliceNumber(ids_with_bsi);

    rhs_slice_number = getValidSliceNumberFromData(rhs_data, rhs_start, rhs_size);

    for (int i = 1; i <= std::min(slice_number, rhs_slice_number); ++i)
    {
        BitmapDataType & rhs_bsi_slice = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start + i]);
        ids_with_bsi[i].roaring_bitmap_with_small_set.rb_or(rhs_bsi_slice.roaring_bitmap_with_small_set);
    }

    if (slice_number < rhs_slice_number)
    {
        for (int i = slice_number + 1; i <= rhs_slice_number; ++i)
        {
            BitmapDataType & rhs_bsi_slice = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start + i]);
            ids_with_bsi[i].roaring_bitmap_with_small_set.rb_or(rhs_bsi_slice.roaring_bitmap_with_small_set);
        }
    }

    max_slice_number = std::max(slice_number, rhs_slice_number);
}

void AggregateFunctionBsiAggData::doMergeOperation(const BSIDataType & rhs_ids_with_bsi)
{
    auto & ids = ids_with_bsi[0];
    const auto & rhs_ids = rhs_ids_with_bsi[0];

    ids.roaring_bitmap_with_small_set.rb_or(rhs_ids.roaring_bitmap_with_small_set);
    int slice_number = max_slice_number;
    int rhs_slice_number = -1;

    if (slice_number == -1)
        slice_number = getValidSliceNumber(ids_with_bsi);

    rhs_slice_number = getValidSliceNumber(rhs_ids_with_bsi);

    slice_number = slice_number == -1 ? 0 : slice_number;
    rhs_slice_number = rhs_slice_number == -1 ? 0 : rhs_slice_number;

    for (int i = 1; i <= std::min(slice_number, rhs_slice_number); ++i)
        ids_with_bsi[i].roaring_bitmap_with_small_set.rb_or(rhs_ids_with_bsi[i].roaring_bitmap_with_small_set);

    if (slice_number < rhs_slice_number)
    {
        for (int i = slice_number + 1; i <= rhs_slice_number; ++i)
            ids_with_bsi[i].roaring_bitmap_with_small_set.rb_or(rhs_ids_with_bsi[i].roaring_bitmap_with_small_set);
    }

    max_slice_number = std::max(slice_number, rhs_slice_number);
}

void AggregateFunctionBsiAggData::merge(const AggregateFunctionBsiAggData & rhs, bool only_merge)
{
    add(rhs.ids_with_bsi, only_merge);
}

void AggregateFunctionBsiAggData::serialize(WriteBuffer & buffer) const
{
    writeVarUInt(ids_with_bsi.size(), buffer);
    for (const auto & slice : ids_with_bsi)
        slice.roaring_bitmap_with_small_set.write(buffer);
}

void AggregateFunctionBsiAggData::deserialize(ReadBuffer & buffer)
{
    size_t size_bsi_slices;
    readVarUInt(size_bsi_slices, buffer);
    for (size_t i = 0; i < size_bsi_slices; ++i)
        ids_with_bsi[i].roaring_bitmap_with_small_set.read(buffer);
}

int AggregateFunctionBsiAggData::getValidSliceNumber(const BSIDataType & data)
{
    int slice_number = -1;
    for (int i = static_cast<int>(data.size()) - 1; i >= 1; --i)
    {
        if (data[i].roaring_bitmap_with_small_set.size())
        {
            slice_number = i;
            break;
        }
    }
    return slice_number == -1 ? 0 : slice_number;
}

int AggregateFunctionBsiAggData::getValidSliceNumberFromData(
    const ColumnAggregateFunction::Container & rhs_data, size_t rhs_start, size_t rhs_size)
{
    int slice_number = -1;
    for (int i = static_cast<int>(rhs_size) - 1; i >= 1; --i)
    {
        BitmapDataType & rhs_bsi_slice = *reinterpret_cast<BitmapDataType *>(rhs_data[rhs_start + i]);

        if (rhs_bsi_slice.roaring_bitmap_with_small_set.size())
        {
            slice_number = i;
            break;
        }
    }

    return slice_number == -1 ? 0 : slice_number;
}

void AggregateFunctionBsiAggData::mergeCommonSlice(
    BitmapDataType & lhs_bsi_slice, const BitmapDataType & rhs_bsi_slice, BitmapDataType & bitmap_carry_out)
{
    BitmapDataType tmp1;
    tmp1.roaring_bitmap_with_small_set.rb_or(lhs_bsi_slice.roaring_bitmap_with_small_set);
    lhs_bsi_slice.roaring_bitmap_with_small_set.rb_xor(rhs_bsi_slice.roaring_bitmap_with_small_set);
    lhs_bsi_slice.roaring_bitmap_with_small_set.rb_xor(bitmap_carry_out.roaring_bitmap_with_small_set);

    BitmapDataType tmp2;
    BitmapDataType tmp3;

    tmp2.roaring_bitmap_with_small_set.rb_or(tmp1.roaring_bitmap_with_small_set);
    tmp2.roaring_bitmap_with_small_set.rb_and(rhs_bsi_slice.roaring_bitmap_with_small_set);

    tmp3.roaring_bitmap_with_small_set.rb_or(bitmap_carry_out.roaring_bitmap_with_small_set);
    tmp3.roaring_bitmap_with_small_set.rb_and(tmp1.roaring_bitmap_with_small_set);

    bitmap_carry_out.roaring_bitmap_with_small_set.rb_and(rhs_bsi_slice.roaring_bitmap_with_small_set);
    bitmap_carry_out.roaring_bitmap_with_small_set.rb_or(tmp3.roaring_bitmap_with_small_set);
    bitmap_carry_out.roaring_bitmap_with_small_set.rb_or(tmp2.roaring_bitmap_with_small_set);
}

void AggregateFunctionBsiAggData::mergeSurplusSlice(BitmapDataType & lhs_bsi_slice, BitmapDataType & bitmap_carry_out)
{
    BitmapDataType tmp;
    tmp.roaring_bitmap_with_small_set.rb_or(lhs_bsi_slice.roaring_bitmap_with_small_set);
    lhs_bsi_slice.roaring_bitmap_with_small_set.rb_xor(bitmap_carry_out.roaring_bitmap_with_small_set);

    bitmap_carry_out.roaring_bitmap_with_small_set.rb_and(tmp.roaring_bitmap_with_small_set);
}

void AggregateFunctionBsiAggData::mergeSurplusSlice(
    BitmapDataType & lhs_bsi_slice, const BitmapDataType & rhs_bsi_slice, BitmapDataType & bitmap_carry_out)
{
    lhs_bsi_slice.roaring_bitmap_with_small_set.rb_or(rhs_bsi_slice.roaring_bitmap_with_small_set);
    lhs_bsi_slice.roaring_bitmap_with_small_set.rb_xor(bitmap_carry_out.roaring_bitmap_with_small_set);

    bitmap_carry_out.roaring_bitmap_with_small_set.rb_and(rhs_bsi_slice.roaring_bitmap_with_small_set);
}

}
