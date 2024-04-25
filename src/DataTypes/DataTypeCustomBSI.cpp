#include "DataTypes/DataTypeCustomBSI.h"
#include "DataTypes/DataTypeCustom.h"
#include "DataTypes/DataTypeFactory.h"

namespace DB
{
void registerDataTypeCustomBSI(DataTypeFactory & factory)
{
    factory.registerSimpleDataTypeCustom("BSI", []
    {
        return std::make_pair(DataTypeFactory::instance().get("Array(AggregateFunction(groupBitmap, UInt64))"), 
                              std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomBSIName>()));
    });
}

}
