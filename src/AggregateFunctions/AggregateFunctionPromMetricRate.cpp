#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPromMetricRate.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

AggregateFunctionPtr createAggregateFunctionPromMetricRate(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertBinary(name, argument_types);
    return std::make_shared<AggregateFunctionPromMetricRate>(argument_types);
}
}

void registerAggregateFunctionPromMetricRate(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true};
    factory.registerFunction(
        "metric_rate", {createAggregateFunctionPromMetricRate, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
