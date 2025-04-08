#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionPromMetricIncrease.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

    AggregateFunctionPtr createAggregateFunctionPromMetricIncrease(
        const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        if (parameters.empty() || parameters.size() != 1)
            throw Exception("Aggregate function " + name + " requires one parameter", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        assertBinary(name, argument_types);

        auto type = parameters[0].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        if ((type == Field::Types::Int64 && parameters[0].get<Int64>() < 0)
            || (type == Field::Types::UInt64 && parameters[0].get<UInt64>() == 0))
            throw Exception("Parameter for aggregate function " + name + " should be positive number", ErrorCodes::BAD_ARGUMENTS);

        Int64 window = parameters[0].get<Int64>();
        return std::make_shared<AggregateFunctionPromMetricIncrease>(argument_types, parameters, window);
    }
}

void registerAggregateFunctionPromMetricIncrease(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = true};
    factory.registerFunction("metric_increase", {createAggregateFunctionPromMetricIncrease, properties}, AggregateFunctionFactory::CaseInsensitive);
}

}
