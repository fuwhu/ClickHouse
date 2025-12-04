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
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Aggregate function {} requires one parameter", name);

    assertBinary(name, argument_types);

    auto type = parameters[0].getType();
    if (type != Field::Types::Int64 && type != Field::Types::UInt64)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

    if ((type == Field::Types::Int64 && parameters[0].safeGet<Int64>() < 0)
        || (type == Field::Types::UInt64 && parameters[0].safeGet<UInt64>() == 0))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Parameter for aggregate function {} should be positive number", name);

    Int64 window = parameters[0].safeGet<Int64>();
    return std::make_shared<AggregateFunctionPromMetricIncrease>(argument_types, parameters, window);
}
}

void registerAggregateFunctionPromMetricIncrease(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = {.returns_default_when_only_null = true};
    factory.registerFunction(
        "metric_increase", {createAggregateFunctionPromMetricIncrease, properties}, AggregateFunctionFactory::Case::Insensitive);
}

}
