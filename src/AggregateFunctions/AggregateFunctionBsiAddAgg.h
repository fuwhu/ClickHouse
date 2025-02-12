#pragma once

#include <memory>
#include <utility>
#include <AggregateFunctions/AggregateFunctionBsiCommon.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeCustomBSI.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <base/types.h>
#include <Common/typeid_cast.h>


namespace DB
{

class AggregateFunctionBsiAddAgg final : public IAggregateFunctionDataHelper<AggregateFunctionBsiAggData, AggregateFunctionBsiAddAgg>
{
public:
    explicit AggregateFunctionBsiAddAgg(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionBsiAggData, AggregateFunctionBsiAddAgg>(argument_types_, {}, createResultType())
    {
    }

    String getName() const override { return "bsi_add_agg"; }

    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr createResultType() const
    {
        auto custom_type = std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomBSIName>());
        return DataTypeFactory::instance().getCustom(std::move(custom_type));
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const ColumnArray & col_arr = typeid_cast<const ColumnArray &>(*columns[0]);
        const ColumnAggregateFunction & col_agg = typeid_cast<const ColumnAggregateFunction &>(col_arr.getData());

        const ColumnArray::Offsets & offsets = col_arr.getOffsets();
        const size_t offset = offsets[row_num - 1];
        auto size = offsets[row_num] - offset;

        data(place).add(col_agg.getData(), offset, size);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override { data(place).merge(data(rhs)); }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buffer, std::optional<size_t> /*version*/) const override
    {
        data(place).serialize(buffer);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buffer, std::optional<size_t> /*version*/, Arena *) const override
    {
        data(place).deserialize(buffer);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = data(place);

        if (!state.ids_with_bsi[0].roaring_bitmap_with_small_set.size())
            return;

        int highest_non_empty_index = -1;

        for (int i = static_cast<int>(state.ids_with_bsi.size()) - 1; i > 0; --i)
        {
            const auto & item = state.ids_with_bsi[i];
            if (item.roaring_bitmap_with_small_set.size())
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

        bitmap_data.roaring_bitmap_with_small_set.merge(state.ids_with_bsi[0].roaring_bitmap_with_small_set);

        if (highest_non_empty_index != -1)
        {
            for (int i = 1; i < highest_non_empty_index + 1; i++)
            {
                const auto & item = state.ids_with_bsi[i];
                arr_to_data.insertDefault();
                AggregateFunctionGroupBitmapData<UInt64> & bitmap_data_idx
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(arr_to_data.getData()[arr_to_data.size() - 1]);
                bitmap_data_idx.roaring_bitmap_with_small_set.merge(item.roaring_bitmap_with_small_set);
            }
        }
    }
};
}
