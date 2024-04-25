#include <AggregateFunctions/AggregateFunctionBsiBuild.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>


namespace DB
{

namespace
{

    AggregateFunctionPtr
    createAggregateFunctionBsiBuild(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
    {
        assertNoParameters(name, parameters);
        assertBinary(name, argument_types);

        return std::make_shared<AggregateFunctionBsiBuild>(argument_types);
    }

}

void registerAggregateFunctionBsiBuild(AggregateFunctionFactory & factory)
{
    factory.registerFunction("bsi_build", createAggregateFunctionBsiBuild, AggregateFunctionFactory::CaseInsensitive);
}

}
