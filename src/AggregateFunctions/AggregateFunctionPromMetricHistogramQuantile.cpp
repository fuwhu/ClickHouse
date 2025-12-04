#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPromMetricHistogramQuantile.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Common/FieldVisitorConvertToNumber.h>


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
            throw Exception(ErrorCodes::PARAMETER_OUT_OF_BOUND, "Quantile level is out of range [0..1]");

        return std::make_shared<AggregateFunctionPromMetricHistogramQuantile>(argument_types, parameters, level);
    }
    else
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0 or 1",
            name);
}
}

void registerAggregateFunctionPromMetricHistogramQuantile(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true};
    factory.registerFunction(
        "metric_histogram_quantile",
        {createAggregateFunctionPromMetricHistogramQuantile, properties},
        AggregateFunctionFactory::Case::Insensitive);
}

}
