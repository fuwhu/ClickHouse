#pragma once

#include "DataTypes/DataTypeCustom.h"
namespace DB
{

class DataTypeCustomBSIName : public DataTypeCustomFixedName
{
public:
    DataTypeCustomBSIName() : DataTypeCustomFixedName("BSI") {}
};

}
