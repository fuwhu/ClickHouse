#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/assert_cast.h>
#include "IO/VarInt.h"

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeCustomBSI.h>
#include <DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS;
}

struct AggregateFunctionBsiBuildData
{
    std::vector<AggregateFunctionGroupBitmapData<UInt64>> bsi_slices;

    AggregateFunctionBsiBuildData() : bsi_slices(65) { }

    static int getHighestNonEmptyIndex(const std::vector<AggregateFunctionGroupBitmapData<UInt64>> & bsi_slices)
    {
        int highest_non_empty_index = -1;

        for (int i = bsi_slices.size() - 1; i > 0; --i)
        {
            const auto & item = bsi_slices[i];
            if (item.rbs.size())
            {
                highest_non_empty_index = i;
                break;
            }
        }

        return highest_non_empty_index;
    }

    void add(const UInt64 & id, const UInt64 & metric)
    {
        if (bsi_slices[0].rbs.rb_contains(id))
            throw Exception("Duplicate ids are not allowed.", ErrorCodes::LOGICAL_ERROR);

        bsi_slices[0].rbs.add(id);

        if (metric == 0)
            return;

        UInt64 tmp_metric = metric;
        size_t index = 1;
        while (tmp_metric)
        {
            if (tmp_metric & 1)
                bsi_slices[index].rbs.add(id);

            tmp_metric = tmp_metric >> 1;
            ++index;
        }
    }

    void merge(const AggregateFunctionBsiBuildData & other)
    {
        if (bsi_slices[0].rbs.rb_and_cardinality(other.bsi_slices[0].rbs))
            throw Exception("Id bitmaps are overlapped.", ErrorCodes::LOGICAL_ERROR);

        for (size_t i = 0; i < 65; ++i)
            bsi_slices[i].rbs.rb_or(other.bsi_slices[i].rbs);
    }

    void serialize(WriteBuffer & buf) const
    {
        int highest_non_empty_index = getHighestNonEmptyIndex(bsi_slices);
        /// if all metric is 0, size = 1(id_bitmap), else size = 1(id_bitmap) +（highest_non_empty_index)
        size_t size = highest_non_empty_index == -1 ? 1 : (1 + highest_non_empty_index);

        writeVarUInt(size, buf);
        for (size_t i = 0; i < size; i++)
            bsi_slices[i].rbs.write(buf);
    }

    void deserialize(ReadBuffer & buf)
    {
        size_t size;
        readVarUInt(size, buf);

        for (size_t i = 0; i < size; i++)
            bsi_slices[i].rbs.read(buf);
    }
};

class AggregateFunctionBsiBuild final : public IAggregateFunctionDataHelper<AggregateFunctionBsiBuildData, AggregateFunctionBsiBuild>
{
private:
    void checkArgumentType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isNativeUInt())
            throw Exception(
                "Aggregate function " + getName() + " only support native uint data type",
                ErrorCodes::AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS);
    }

    static UInt64 getUInt64Value(const IColumn & column, const size_t & row_num)
    {
        WhichDataType which(column.getDataType());

        UInt64 value;
        if (which.isUInt8())
            value = static_cast<const ColumnVector<UInt8> &>(column).getData()[row_num];
        else if (which.isUInt16())
            value = static_cast<const ColumnVector<UInt16> &>(column).getData()[row_num];
        else if (which.isUInt32())
            value = static_cast<const ColumnVector<UInt32> &>(column).getData()[row_num];
        else
            value = static_cast<const ColumnVector<UInt64> &>(column).getData()[row_num];

        return value;
    }

public:
    explicit AggregateFunctionBsiBuild(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<AggregateFunctionBsiBuildData, AggregateFunctionBsiBuild>(argument_types_, {})
    {
        checkArgumentType(argument_types_[0]);
        checkArgumentType(argument_types_[1]);
    }

    String getName() const override { return "bsi_build"; }

    bool allocatesMemoryInArena() const override { return false; }

    DataTypePtr getReturnType() const override
    {
        auto custom_type = std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomBSIName>());
        return DataTypeFactory::instance().getCustom(std::move(custom_type));
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        UInt64 id = getUInt64Value(*columns[0], row_num);
        UInt64 metric = getUInt64Value(*columns[1], row_num);

        this->data(place).add(id, metric);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override { this->data(place).merge(this->data(rhs)); }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        this->data(place).serialize(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        this->data(place).deserialize(buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);

        int highest_non_empty_index = AggregateFunctionBsiBuildData::getHighestNonEmptyIndex(state.bsi_slices);

        /// if all metric is 0, size = 1(id_bitmap), else size = 1(id_bitmap) +（highest_non_empty_index)
        size_t size = highest_non_empty_index == -1 ? 1 : (1 + highest_non_empty_index);

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
