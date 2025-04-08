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
    extern const int PARAMETER_OUT_OF_BOUND;
}

struct AggregateFunctionMetricHistogramQuantileData
{
    std::map<Float64, Float64> histogram;
};

/*
    `histogram_quantile` is a promethus function (see https://prometheus.io/docs/prometheus/latest/querying/functions/#histogram_quantile for detail).
    For the meaning of each argument and the principle of this function, you can check the promethus document and the links below:
    a. https://www.cnblogs.com/ryanyangcs/p/11309373.html
    b. https://git.bilibili.co/ops/moni/metric-udf-iceberg/-/blob/main/src/main/java/com/bilibili/udaf/MetricHistogramQuantileV11.java
*/
class AggregateFunctionPromMetricHistogramQuantile final
    : public IAggregateFunctionDataHelper<AggregateFunctionMetricHistogramQuantileData, AggregateFunctionPromMetricHistogramQuantile>
{
private:
    Float64 level;

    // check the data type of the `le` argument, note that the `le` is a specific argument of `histogram_quantile` function.
    void checkArgLeType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isString())
            throw Exception(
                "Aggregate function " + getName() + " le argument only support string data type", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    //// check the data type of the `value` argument, TODO:: note that the `value` here is actually the count of each bucket in the historam, so we can optimize it to use integer data type instead here.
    void checkArgValueType(const DataTypePtr & data_type) const
    {
        WhichDataType which(data_type->getTypeId());

        if (!which.isFloat() && !which.isDecimal())
            throw Exception(
                "Aggregate function " + getName() + " value argument only support float or decimal data type",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    explicit AggregateFunctionPromMetricHistogramQuantile(const DataTypes & arguments, const Array & parameters_, Float64 level_ = 0.0)
        : IAggregateFunctionDataHelper<AggregateFunctionMetricHistogramQuantileData, AggregateFunctionPromMetricHistogramQuantile>(
            arguments, parameters_)
        , level(level_)
    {
        checkArgLeType(arguments[0]);
        checkArgValueType(arguments[1]);
    }

    String getName() const override { return "metric_histogram_quantile"; }

    DataTypePtr getReturnType() const override { return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()); }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        StringRef le_str = columns[0]->getDataAt(row_num);
        Float64 count = columns[1]->getFloat64(row_num);

        Float64 le;
        if (le_str == "+Inf" || le_str == "+INF")
            le = std::numeric_limits<Float64>::max();
        else
            le = std::stod(le_str.data);

        auto & state = this->data(place);
        state.histogram[le] += count;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & state = this->data(place);
        const auto & rhs_state = this->data(rhs);

        for (const auto & entry : rhs_state.histogram)
            state.histogram[entry.first] += entry.second;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        const auto & state = this->data(place);
        writeBinary(state.histogram.size(), buf);
        for (const auto & entry : state.histogram)
        {
            writeBinary(entry.first, buf);
            writeBinary(entry.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & state = this->data(place);
        size_t size;
        readBinary(size, buf);
        for (size_t i = 0; i < size; ++i)
        {
            Float64 key, value;
            readBinary(key, buf);
            readBinary(value, buf);
            state.histogram[key] += value;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = this->data(place);
        auto & col = assert_cast<ColumnNullable &>(to);

        if (state.histogram.empty())
        {
            col.insertDefault();
            return;
        }

        Float64 total_count = state.histogram.rbegin()->second;
        if (total_count == 0)
        {
            col.insertDefault();
            return;
        }

        Float64 target_count = level * total_count;

        Float64 last_le = 0;
        Float64 last_count = 0;

        auto & nested_data = col.getNestedColumn();
        auto & null_map = col.getNullMapColumn();

        for (const auto & entry : state.histogram)
        {
            if (entry.first == std::numeric_limits<Float64>::max())
            {
                assert_cast<ColumnFloat64 &>(nested_data).getData().push_back(last_le);
                null_map.getData().push_back(0);
                return;
            }

            if (target_count <= entry.second)
            {
                Float64 whole_diff = entry.second - last_count;
                Float64 diff = target_count - last_count;
                Float64 k = diff / whole_diff;
                Float64 res = last_le + (entry.first - last_le) * k;
                assert_cast<ColumnFloat64 &>(nested_data).getData().push_back(res);
                null_map.getData().push_back(0);
                return;
            }
            else
            {
                last_le = entry.first;
                last_count = entry.second;
            }
        }

        assert_cast<ColumnFloat64 &>(nested_data).getData().push_back(last_le);
        null_map.getData().push_back(0);
    }
};

}
