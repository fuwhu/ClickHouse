#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include "Common/assert_cast.h"

#include "Columns/ColumnNullable.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypesNumber.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

struct AggregateFunctionMetricRateData
{
    Int64 min_time = std::numeric_limits<Int64>::max();
    Int64 max_time = std::numeric_limits<Int64>::min();
    Float64 min_value = 0.0;
    Float64 max_value = 0.0;
};

/*
    rate is a promethus function (see https://prometheus.io/docs/prometheus/latest/querying/functions/#rate for detail).
*/
class AggregateFunctionPromMetricRate final
    : public IAggregateFunctionDataHelper<AggregateFunctionMetricRateData, AggregateFunctionPromMetricRate>
{
private:
    void checkArgTimeType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isInt32() && !which.isInt64())
            throw Exception(
                "Aggregate function " + getName() + " time argument only support int32 or int64 data type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void checkArgValueType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isFloat() && !which.isDecimal())
            throw Exception(
                "Aggregate function " + getName() + " value argument only support float or decimal data type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    explicit AggregateFunctionPromMetricRate(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionMetricRateData, AggregateFunctionPromMetricRate>(arguments, {})
    {
        checkArgTimeType(arguments[0]);
        checkArgValueType(arguments[1]);
    }

    String getName() const override { return "metric_rate"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Float64 time = columns[0]->getFloat64(row_num);
        Float64 value = columns[1]->getFloat64(row_num);
        auto & state = this->data(place);

        if (time > state.max_time)
        {
            state.max_time = time;
            state.max_value = value;
        }

        if (time < state.min_time)
        {
            state.min_time = time;
            state.min_value = value;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);
        if (rhs_state.max_time > state.max_time)
        {
            state.max_time = rhs_state.max_time;
            state.max_value = rhs_state.max_value;
        }

        if (rhs_state.min_time < state.min_time)
        {
            state.min_time = rhs_state.min_time;
            state.min_value = rhs_state.min_value;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);
        writeBinary(state.min_time, buf);
        writeBinary(state.max_time, buf);
        writeBinary(state.min_value, buf);
        writeBinary(state.max_value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = this->data(place);
        readBinary(state.min_time, buf);
        readBinary(state.max_time, buf);
        readBinary(state.min_value, buf);
        readBinary(state.max_value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);
        auto & col = assert_cast<ColumnNullable &>(to);

        if (state.min_time >= state.max_time)
        {
            col.insertDefault();
            return;
        }

        Int64 time_diff = state.max_time - state.min_time;
        Float64 result = (state.max_value - state.min_value) / time_diff * 1000;
        auto & nested_data = col.getNestedColumn();
        assert_cast<ColumnFloat64 &>(nested_data).getData().push_back(result);
        auto & null_map = col.getNullMapColumn();
        null_map.getData().push_back(0);
    }
};

}
