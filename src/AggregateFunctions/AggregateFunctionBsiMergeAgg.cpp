#include <memory>
#include <AggregateFunctions/AggregateFunctionBsiMergeAgg.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <DataTypes/IDataType.h>

namespace DB
{

namespace
{
AggregateFunctionPtr
createAggregateFunctionBsiMergeAgg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    return std::make_shared<AggregateFunctionBsiMergeAgg>(argument_types);
}
}
void registerAggregateFunctionBsiMergeAgg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("bsi_merge_agg", createAggregateFunctionBsiMergeAgg, AggregateFunctionFactory::Case::Insensitive);
}

}
