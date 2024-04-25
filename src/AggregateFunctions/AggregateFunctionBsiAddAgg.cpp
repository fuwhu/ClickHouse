#include <memory>
#include "AggregateFunctions/AggregateFunctionBsiAddAgg.h"
#include "AggregateFunctions/AggregateFunctionFactory.h"
#include "AggregateFunctions/FactoryHelpers.h"
#include "AggregateFunctions/IAggregateFunction.h"
#include "Core/Field.h"
#include "DataTypes/IDataType.h"
#include "DataTypes/Serializations/ISerialization.h"
namespace DB
{


namespace  
{
AggregateFunctionPtr createAggregateFunctionBsiAddAgg(const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertNoParameters(name, parameters);
    assertUnary(name, argument_types);

    return std::make_shared<AggregateFunctionBsiAddAgg>(argument_types);
}
}
void registerAggregateFunctionBsiAddAgg(AggregateFunctionFactory & factory)
{
    factory.registerFunction("bsi_add_agg", createAggregateFunctionBsiAddAgg, AggregateFunctionFactory::CaseInsensitive);
}

}
