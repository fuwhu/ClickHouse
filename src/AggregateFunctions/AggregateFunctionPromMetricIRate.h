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

struct AggregateFunctionMetricIRateData
{
    Int64 second_to_last_time = std::numeric_limits<Int64>::min();
    Int64 last_time = std::numeric_limits<Int64>::min();
    Float64 second_to_last_value = 0.0;
    Float64 last_value = 0.0;
};

/*
    irate is a promethus function (see https://prometheus.io/docs/prometheus/latest/querying/functions/#irate for detail).
*/
class AggregateFunctionPromMetricIRate final
    : public IAggregateFunctionDataHelper<AggregateFunctionMetricIRateData, AggregateFunctionPromMetricIRate>
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
    explicit AggregateFunctionPromMetricIRate(const DataTypes & arguments)
        : IAggregateFunctionDataHelper<AggregateFunctionMetricIRateData, AggregateFunctionPromMetricIRate>(arguments, {})
    {
        checkArgTimeType(arguments[0]);
        checkArgValueType(arguments[1]);
    }

    String getName() const override { return "metric_irate"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        Float64 time = columns[0]->getFloat64(row_num);
        Float64 value = columns[1]->getFloat64(row_num);
        auto & state = this->data(place);

        if (time > state.last_time)
        {
            if (state.last_time != std::numeric_limits<Int64>::min())
            {
                state.second_to_last_time = state.last_time;
                state.second_to_last_value = state.last_value;
            }

            state.last_time = time;
            state.last_value = value;
        }
        else if (time > state.second_to_last_time)
        {
            state.second_to_last_time = time;
            state.second_to_last_value = value;
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);
        
        std::vector<std::pair<Int64, Float64>> points;

        if (state.last_time != std::numeric_limits<Int64>::min())
        {
            points.emplace_back(state.last_time, state.last_value);
            if (state.second_to_last_time != std::numeric_limits<Int64>::min())
                points.emplace_back(state.second_to_last_time, state.second_to_last_value);
        }

        if (rhs_state.last_time != std::numeric_limits<Int64>::min())
        {
            points.emplace_back(rhs_state.last_time, rhs_state.last_value);
            if (rhs_state.second_to_last_time != std::numeric_limits<Int64>::min())
                points.emplace_back(rhs_state.second_to_last_time, rhs_state.second_to_last_value);
        }

        if (points.empty())
            return;

        std::sort(points.begin(), points.end(), [](const auto & a, const auto & b) {
            return a.first > b.first;
        });

        state.last_time = points[0].first;
        state.last_value = points[0].second;

        if (points.size() >= 2)
        {
            state.second_to_last_time = points[1].first;
            state.second_to_last_value = points[1].second;
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);
        writeBinary(state.second_to_last_time, buf);
        writeBinary(state.last_time, buf);
        writeBinary(state.second_to_last_value, buf);
        writeBinary(state.last_value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = this->data(place);
        readBinary(state.second_to_last_time, buf);
        readBinary(state.last_time, buf);
        readBinary(state.second_to_last_value, buf);
        readBinary(state.last_value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto & state = this->data(place);
        auto & col = assert_cast<ColumnNullable &>(to);

        if (state.second_to_last_time == std::numeric_limits<Int64>::min() || state.last_time == state.second_to_last_time)
        {
            col.insertDefault();
            return;
        }

        Int64 time_diff = state.last_time - state.second_to_last_time;
        Float64 result = (state.last_value - state.second_to_last_value) / time_diff * 1000;
        auto & nested_data = col.getNestedColumn();
        assert_cast<ColumnFloat64 &>(nested_data).getData().push_back(result);
        auto & null_map = col.getNullMapColumn();
        null_map.getData().push_back(0);
    }
};

}
