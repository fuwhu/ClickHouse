namespace DB
{
class FunctionFactory;

void registerFunctionPolarisArgsParse(FunctionFactory &);

void registerUDFs(FunctionFactory & factory)
{
    registerFunctionPolarisArgsParse(factory);
}
}
