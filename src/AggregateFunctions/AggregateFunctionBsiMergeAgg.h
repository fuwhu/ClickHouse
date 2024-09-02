#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <Poco/Logger.h>
#include "DataTypes/DataTypeCustom.h"
#include "DataTypes/DataTypeCustomBSI.h"
#include "DataTypes/DataTypeFactory.h"
#include "base/logger_useful.h"
#include "Common/Arena.h"
#include "Common/typeid_cast.h"
#include "AggregateFunctions/AggregateFunctionFactory.h"
#include "AggregateFunctions/AggregateFunctionGroupBitmapData.h"
#include "AggregateFunctions/IAggregateFunction.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnAggregateFunction.h"
#include "Columns/IColumn.h"
#include "Core/Field.h"
#include "DataTypes/DataTypeAggregateFunction.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/Serializations/ISerialization.h"
#include "IO/ReadBuffer.h"
#include "IO/WriteBuffer.h"
#include "base/types.h"



namespace DB
{

struct AggregateFunctionBsiMergeAggData
{
    std::vector<AggregateFunctionGroupBitmapData<UInt64>> bsi_slices;
    int max_number = -1;

    AggregateFunctionBsiMergeAggData() : bsi_slices(65) {}

    void add(const DB::ColumnAggregateFunction::Container & data, const size_t & start, const size_t & size)
    {
        /// Only for scenarios where the metric value of the same id is unchanged, the current version does not impose strong restrictions.
        
        /// TODO restriction, the metric value of the same id is unchanged.
        /// bsi a: [[1,2,3,4], [1,2], [], [1,2]] ...
        /// bsi b: [[1,2], [1,2], [], [1,2]] ...
        /// not null intersection: a_b_i = a[0] and b[0]
        /// (a_b_i and a[i]) must equals to (a_b_i and b[i]), i > 0.

        doMergeOperation(data, start, size);
    }

    void add(const std::vector<AggregateFunctionGroupBitmapData<UInt64>> & rhs_bsi_slices)
    {
        /// Only for scenarios where the metric value of the same id is unchanged, the current version does not impose strong restrictions.
        
        /// TODO restriction, the metric value of the same id is unchanged.
        /// bsi a: [[1,2,3,4], [1,2], [], [1,2]] ...
        /// bsi b: [[1,2], [1,2], [], [1,2]] ...
        /// not null intersection: a_b_i = a[0] and b[0]
        /// (a_b_i and a[i]) must equals to (a_b_i and b[i]), i > 0.
        doMergeOperation(rhs_bsi_slices);
    }

    void doMergeOperation(const DB::ColumnAggregateFunction::Container & data, const size_t & start, const size_t & size)
    {
        AggregateFunctionGroupBitmapData<UInt64> & not_null_rb = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(data[start]); 

        bsi_slices[0].rbs.rb_or(not_null_rb.rbs);
        int max_slice_number = max_number;
        int rhs_max_slice_number = size - 1;

        if (max_slice_number == -1)
        {
            for (int i = bsi_slices.size() - 1; i > 0; --i)
            {
                if (bsi_slices[i].rbs.size())
                {
                    max_slice_number = i;
                    break;
                }
            }
        }

        max_slice_number = max_slice_number == -1 ? 1 : max_slice_number;

        for (int i = 1; i <= std::min(max_slice_number, rhs_max_slice_number); ++i)
        {
            AggregateFunctionGroupBitmapData<UInt64> & bsi_slice = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(data[start + i]); 
            bsi_slices[i].rbs.rb_or(bsi_slice.rbs);
        }

        if (max_slice_number < rhs_max_slice_number)
        {
            for (int i = max_slice_number + 1; i <= rhs_max_slice_number; ++i)
            {
                AggregateFunctionGroupBitmapData<UInt64> & bsi_slice = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(data[start + i]); 
                bsi_slices[i].rbs.rb_or(bsi_slice.rbs);
            }
        } 

        max_number = std::max(max_slice_number, rhs_max_slice_number);
    }

    void doMergeOperation(const std::vector<AggregateFunctionGroupBitmapData<UInt64>> & rhs_bsi_slices)
    {
        bsi_slices[0].rbs.rb_or(rhs_bsi_slices[0].rbs);
        int max_slice_number = max_number;
        int rhs_max_slice_number = rhs_bsi_slices.size() - 1;

        if (max_slice_number == -1)
        {
            for (int i = bsi_slices.size() - 1; i > 0; --i)
            {
                if (bsi_slices[i].rbs.size())
                {
                    max_slice_number = i;
                    break;
                }
            }
        }

        max_slice_number = max_slice_number == -1 ? 1 : max_slice_number;

        for (int i = 1; i <= std::min(max_slice_number, rhs_max_slice_number); ++i)
        {
            bsi_slices[i].rbs.rb_or(rhs_bsi_slices[i].rbs);
        }

        if (max_slice_number < rhs_max_slice_number)
        {
            for (int i = max_slice_number + 1; i <= rhs_max_slice_number; ++i)
            {
                bsi_slices[i].rbs.rb_or(rhs_bsi_slices[i].rbs);
            }
        } 

        max_number = std::max(max_slice_number, rhs_max_slice_number);
    }

    void merge(const AggregateFunctionBsiMergeAggData & rhs)
    {
        add(rhs.bsi_slices);
    }

    void serialize(WriteBuffer & buffer) const
    {
        writeVarUInt(bsi_slices.size(), buffer);
        for (const auto & slice : bsi_slices)
            slice.rbs.write(buffer);
    }   

    void deserialize(ReadBuffer & buffer)
    {
        size_t size_bsi_slices;
        readVarUInt(size_bsi_slices, buffer);
        for (size_t i = 0; i < size_bsi_slices; ++i)
        {
            bsi_slices[i].rbs.read(buffer);
        }
    }

};

class AggregateFunctionBsiMergeAgg final : public IAggregateFunctionDataHelper<AggregateFunctionBsiMergeAggData, AggregateFunctionBsiMergeAgg>
{
public:
    explicit AggregateFunctionBsiMergeAgg(const DataTypes & argument_types_) : IAggregateFunctionDataHelper<AggregateFunctionBsiMergeAggData, AggregateFunctionBsiMergeAgg>(argument_types_, {}) {}

    String getName() const override { return "bsi_merge_agg"; }

    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        auto custom_type = std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomBSIName>());
        return DataTypeFactory::instance().getCustom(std::move(custom_type));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const ColumnArray & col_arr = typeid_cast<const ColumnArray &>(*columns[0]);
        const ColumnAggregateFunction & col_agg = typeid_cast<const ColumnAggregateFunction &>(col_arr.getData());

        const ColumnArray::Offsets & offsets = col_arr.getOffsets();
        const size_t offset = offsets[row_num - 1];
        auto size = offsets[row_num] - offset;

        this->data(place).add(col_agg.getData(), offset, size);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buffer, std::optional<size_t> /*version*/) const override
    {
        this->data(place).serialize(buffer);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buffer, std::optional<size_t> /*version*/, Arena *) const override
    {
        this->data(place).deserialize(buffer);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);

        if (!state.bsi_slices[0].rbs.size())
            return;
        
        int highest_non_empty_index = -1;

        for (int i = state.bsi_slices.size() - 1; i > 0; --i)
        {
            const auto & item = state.bsi_slices[i];
            if (item.rbs.size())
            {
                highest_non_empty_index = i;
                break;
            }
        }

        /// if all metric is 0, size = 1(id_bitmap), else size = 1(id_bitmap) + highest_non_empty_index
        size_t size = highest_non_empty_index == -1 ? 1 : highest_non_empty_index + 1;

        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & arr_to_offsets = arr_to.getOffsets();
        ColumnAggregateFunction & arr_to_data = typeid_cast<ColumnAggregateFunction &>(arr_to.getData());

        arr_to_offsets.push_back(arr_to_offsets.back() + size);

        arr_to_data.insertDefault();
        AggregateFunctionGroupBitmapData<UInt64> & bitmap_data
            = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(arr_to_data.getData()[arr_to_data.size() - 1]);

        bitmap_data.rbs.merge(state.bsi_slices[0].rbs);

        if (highest_non_empty_index != -1)
        {
            for (int i = 1; i < highest_non_empty_index + 1; i++)
            {
                const auto & item = state.bsi_slices[i];
                arr_to_data.insertDefault();
                AggregateFunctionGroupBitmapData<UInt64> & bitmap_data_idx
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(arr_to_data.getData()[arr_to_data.size() - 1]);
                bitmap_data_idx.rbs.merge(item.rbs);
            }
        }
    }
};

}
