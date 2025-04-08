#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPromMetricHistogramQuantile.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include "Common/FieldVisitorConvertToNumber.h"


namespace DB
{

namespace
{

    AggregateFunctionPtr createAggregateFunctionPromMetricHistogramQuantile(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertBinary(name, argument_types);

        if (parameters.empty())
            return std::make_shared<AggregateFunctionPromMetricHistogramQuantile>(argument_types, parameters);
        else if (parameters.size() == 1)
        {
            Float64 level = applyVisitor(FieldVisitorConvertToNumber<Float64>(), parameters[0]);

            if (isNaN(level) || level < 0 || level > 1)
                throw Exception("Quantile level is out of range [0..1]", ErrorCodes::PARAMETER_OUT_OF_BOUND);

            return std::make_shared<AggregateFunctionPromMetricHistogramQuantile>(argument_types, parameters, level);
        }
        else
            throw Exception(
                "Incorrect number of parameters for aggregate function " + name + ", should be 0 or 1",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
    }
}

void registerAggregateFunctionPromMetricHistogramQuantile(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true};
    factory.registerFunction(
        "metric_histogram_quantile", {createAggregateFunctionPromMetricHistogramQuantile, properties}, AggregateFunctionFactory::CaseInsensitive);
}

}
