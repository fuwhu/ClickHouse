#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPromMetricIRate.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

    AggregateFunctionPtr createAggregateFunctionPromMetricIRate(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertBinary(name, argument_types);
        return std::make_shared<AggregateFunctionPromMetricIRate>(argument_types);
    }
}

void registerAggregateFunctionPromMetricIRate(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true};
    factory.registerFunction("metric_irate", {createAggregateFunctionPromMetricIRate, properties}, AggregateFunctionFactory::CaseInsensitive);
}

}
