#include "FunctionFactory.h"
#include "MultiMatchAnyImpl.h"


namespace DB
{
void registerFunctionMultiMatchAny(FunctionFactory & factory)
{
    factory.registerFunction<FunctionMultiMatchAny>();
}

}
