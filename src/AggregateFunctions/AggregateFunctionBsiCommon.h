#pragma once

#include <vector>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <base/types.h>

namespace DB
{

using BitmapDataType = AggregateFunctionGroupBitmapData<UInt64>;
using BSIDataType = std::vector<AggregateFunctionGroupBitmapData<UInt64>>;

struct AggregateFunctionBsiAggData
{
    BSIDataType ids_with_bsi;
    int max_slice_number = -1;

    AggregateFunctionBsiAggData();

    /**
     * only_merge = true
     *
     * Only for scenarios where the metric value of the same id is unchanged, the current version does not impose strong restrictions.
     * TODO restriction, the metric value of the same id is unchanged.
     * bsi a: [[1,2,3,4], [1,2], [], [1,2]] ...
     * bsi b: [[1,2], [1,2], [], [1,2]] ...
     * not null intersection: a_b_i = a[0] and b[0]
     * (a_b_i and a[i]) must equals to (a_b_i and b[i]), i > 0.
     */

    void
    add(const ColumnAggregateFunction::Container & rhs_data, const size_t & rhs_start, const size_t & rhs_size, bool only_merge = false);

    void add(const BSIDataType & rhs_ids_with_bsi, bool only_merge = false);

    void doMergeOperation(const ColumnAggregateFunction::Container & rhs_data, const size_t & rhs_start, const size_t & rhs_size);

    void doMergeOperation(const BSIDataType & rhs_ids_with_bsi);

    void merge(const AggregateFunctionBsiAggData & rhs, bool only_merge = false);

    void serialize(WriteBuffer & buffer) const;

    void deserialize(ReadBuffer & buffer);

    static int getValidSliceNumber(const BSIDataType & data);

    static int getValidSliceNumberFromData(const ColumnAggregateFunction::Container & rhs_data, size_t rhs_start, size_t rhs_size);

    static void mergeCommonSlice(BitmapDataType & lhs_bsi_slice, const BitmapDataType & rhs_bsi_slice, BitmapDataType & bitmap_carry_out);

    static void mergeSurplusSlice(BitmapDataType & lhs_bsi_slice, BitmapDataType & bitmap_carry_out);

    static void mergeSurplusSlice(BitmapDataType & lhs_bsi_slice, const BitmapDataType & rhs_bsi_slice, BitmapDataType & bitmap_carry_out);
};

}
