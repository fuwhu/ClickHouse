#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionWindowFunnel.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <base/range.h>
#include <DataTypes/DataTypeArray.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

template <template <typename, typename, bool> class AggregateFunctionTemplate, template <typename> class Data, bool is_array, typename... TArgs>
IAggregateFunction * createWindowFunnelWithUnsignedIntegerType(const IDataType & argument_type, TArgs &&... args)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::UInt8)
        return new AggregateFunctionTemplate<UInt8, Data<UInt8>, is_array>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt16)
        return new AggregateFunctionTemplate<UInt16, Data<UInt16>, is_array>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt32)
        return new AggregateFunctionTemplate<UInt32, Data<UInt32>, is_array>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt64)
        return new AggregateFunctionTemplate<UInt64, Data<UInt64>, is_array>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt128)
        return new AggregateFunctionTemplate<UInt128, Data<UInt128>, is_array>(std::forward<TArgs>(args)...);
    if (which.idx == TypeIndex::UInt256)
        return new AggregateFunctionTemplate<UInt256, Data<UInt256>, is_array>(std::forward<TArgs>(args)...);
    return nullptr;
}

template <template <typename> class Data>
AggregateFunctionPtr
createAggregateFunctionWindowFunnel(const std::string & name, const DataTypes & arguments, const Array & params, const Settings *)
{
    if (params.empty())
        throw Exception{"Aggregate function " + name + " requires at least one parameter: <window>, [option, [option, ...]]", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    if (arguments.size() < 2)
        throw Exception("Aggregate function " + name + " requires one timestamp argument and at least one event condition.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    if (arguments.size() > max_events + 1)
        throw Exception("Too many event arguments for aggregate function " + name, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    for (const auto i : collections::range(1, arguments.size()))
    {
        const auto * cond_arg = arguments[i].get();
        if (!isUInt8(cond_arg))
            throw Exception{"Illegal type " + cond_arg->getName() + " of argument " + toString(i + 1) + " of aggregate function "
                    + name + ", must be UInt8", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    }

    AggregateFunctionPtr res(createWindowFunnelWithUnsignedIntegerType<AggregateFunctionWindowFunnel, Data, false>(*arguments[0], arguments, params));
    WhichDataType which(arguments.front().get());
    if (res)
        return res;
    else if (which.isDate())
        return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>, false>>(arguments, params);
    else if (which.isDateTime())
        return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>, false>>(arguments, params);
    else if (which.isArray())
    {
        // get nested data type
        const auto * type_array = typeid_cast<const DataTypeArray *>(arguments.front().get());
        WhichDataType nested_type(type_array->getNestedType());
        if (nested_type.isUInt())
            return AggregateFunctionPtr(createWindowFunnelWithUnsignedIntegerType<AggregateFunctionWindowFunnel, Data, true>(
                *type_array->getNestedType().get(), arguments, params));
        else if ((nested_type.isDate()))
            return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDate::FieldType, Data<DataTypeDate::FieldType>, true>>(
                arguments, params);
        else if (nested_type.isDateTime())
            return std::make_shared<AggregateFunctionWindowFunnel<DataTypeDateTime::FieldType, Data<DataTypeDateTime::FieldType>, true>>(
                arguments, params);
    }

    throw Exception{"Illegal type " + arguments.front().get()->getName()
            + " of first argument of aggregate function " + name + ", must be Unsigned Number, Date, DateTime or Array with above nested types",
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
}

}

void registerAggregateFunctionWindowFunnel(AggregateFunctionFactory & factory)
{
    factory.registerFunction("windowFunnel", createAggregateFunctionWindowFunnel<AggregateFunctionWindowFunnelData>);
}

}
