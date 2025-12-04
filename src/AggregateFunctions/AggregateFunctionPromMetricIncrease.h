#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnNullable.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int BAD_ARGUMENTS;
}

struct AggregateFunctionMetricIncreaseData
{
    Int64 min_time = std::numeric_limits<Int64>::max();
    Int64 max_time = std::numeric_limits<Int64>::min();
    Float64 min_value = 0.0;
    Float64 max_value = 0.0;
};

/*
    increase is a promethus function (see https://prometheus.io/docs/prometheus/latest/querying/functions/#increase for detail).
*/
class AggregateFunctionPromMetricIncrease final
    : public IAggregateFunctionDataHelper<AggregateFunctionMetricIncreaseData, AggregateFunctionPromMetricIncrease>
{
private:
    Int64 window;

    void checkArgTimeType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isInt32() && !which.isInt64())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} time argument only support int32 or int64 data type",
                getName());
    }

    void checkArgValueType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isFloat() && !which.isDecimal())
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {} value argument only support float or decimal data type",
                getName());
    }

public:
    explicit AggregateFunctionPromMetricIncrease(const DataTypes & arguments, const Array & parameters_, UInt64 window_)
        : IAggregateFunctionDataHelper<AggregateFunctionMetricIncreaseData, AggregateFunctionPromMetricIncrease>(
              arguments, parameters_, createResultType())
        , window(window_)
    {
        checkArgTimeType(arguments[0]);
        checkArgValueType(arguments[1]);
    }

    String getName() const override { return "metric_increase"; }

    DataTypePtr createResultType() const { return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Int64 time = columns[0]->getInt(row_num);
        Float64 value = columns[1]->getFloat64(row_num);
        auto & state = data(place);

        if (time < state.min_time)
        {
            state.min_time = time;
            state.min_value = value;
        }

        if (time > state.max_time)
        {
            state.max_time = time;
            state.max_value = value;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = data(place);
        const auto & rhs_state = data(rhs);

        if (rhs_state.min_time < state.min_time)
        {
            state.min_time = rhs_state.min_time;
            state.min_value = rhs_state.min_value;
        }

        if (rhs_state.max_time > state.max_time)
        {
            state.max_time = rhs_state.max_time;
            state.max_value = rhs_state.max_value;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = data(place);
        writeBinary(state.min_time, buf);
        writeBinary(state.max_time, buf);
        writeBinary(state.min_value, buf);
        writeBinary(state.max_value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = data(place);
        readBinary(state.min_time, buf);
        readBinary(state.max_time, buf);
        readBinary(state.min_value, buf);
        readBinary(state.max_value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = data(place);
        auto & col = assert_cast<ColumnNullable &>(to);

        if (state.min_time >= state.max_time)
        {
            col.insertDefault();
            return;
        }

        Int64 time_diff = state.max_time - state.min_time;
        Float64 rate = (state.max_value - state.min_value) / time_diff;
        rate = std::max(rate, 0.0);
        Float64 result = rate * window;

        auto & nested_data = col.getNestedColumn();
        assert_cast<ColumnFloat64 &>(nested_data).getData().push_back(result);
        auto & null_map = col.getNullMapColumn();
        null_map.getData().push_back(0);
    }
};

}
