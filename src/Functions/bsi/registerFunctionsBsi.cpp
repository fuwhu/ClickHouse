#include "Functions/bsi/FunctionBsiFilter.h"
#include "Functions/bsi/FunctionBsiOperator.h"
#include "Functions/bsi/FunctionBsiSum.h"
#include "Functions/bsi/FunctionBsiTopK.h"
#include "Functions/bsi/FunctionBsiProductSum.h"
#include "Functions/bsi/FunctionBsiSquareSum.h"

namespace DB
{
class FunctionFactory;

void registerFunctionsBsi(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBsiGt>();
    factory.registerFunction<FunctionBsiLt>();
    factory.registerFunction<FunctionBsiGe>();
    factory.registerFunction<FunctionBsiLe>();
    factory.registerFunction<FunctionBsiRange>();
    factory.registerFunction<FunctionBsiTopK>();
    factory.registerFunction<FunctionBsiSum>();
    factory.registerFunction<FunctionBsiFilter>();
    factory.registerFunction<FunctionBsiProductSum>();
    factory.registerFunction<FunctionBsiSquareSum>();
}

}
