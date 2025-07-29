#pragma once

#include <algorithm>
#include <memory>
#include <utility>
#include <Poco/Logger.h>
#include "AggregateFunctions/AggregateFunctionBsiCommon.h"
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

class AggregateFunctionBsiMergeAgg final : public IAggregateFunctionDataHelper<AggregateFunctionBsiAggData, AggregateFunctionBsiMergeAgg>
{
public:
    explicit AggregateFunctionBsiMergeAgg(const DataTypes & argument_types_) : IAggregateFunctionDataHelper<AggregateFunctionBsiAggData, AggregateFunctionBsiMergeAgg>(argument_types_, {}) {}

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

        this->data(place).add(col_agg.getData(), offset, size, true);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs), true);
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
        
        int highest_non_empty_index = -1;

        for (int i = state.ids_with_bsi.size() - 1; i > 0; --i)
        {
            const auto & item = state.ids_with_bsi[i];
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

        bitmap_data.rbs.merge(state.ids_with_bsi[0].rbs);

        if (highest_non_empty_index != -1)
        {
            for (int i = 1; i < highest_non_empty_index + 1; i++)
            {
                const auto & item = state.ids_with_bsi[i];
                arr_to_data.insertDefault();
                AggregateFunctionGroupBitmapData<UInt64> & bitmap_data_idx
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(arr_to_data.getData()[arr_to_data.size() - 1]);
                bitmap_data_idx.rbs.merge(item.rbs);
            }
        }
    }
};

}
