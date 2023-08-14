#include <Columns/ColumnMapV2.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeMapV2.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationMapV2.h>
#include <Formats/FormatSettings.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/IAST.h>
#include <base/map.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/assert_cast.h>
#include <Common/quoteString.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}


DataTypeMapV2::DataTypeMapV2(const DataTypes & elems_)
{
    assert(elems_.size() == 2);
    key_type = elems_[0];
    value_type = elems_[1];

    assertKeyType();

    nested = std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{key_type, value_type}, Names{"keys", "values"}));
}

DataTypeMapV2::DataTypeMapV2(const DataTypePtr & key_type_, const DataTypePtr & value_type_)
    : key_type(key_type_)
    , value_type(value_type_)
    , nested(std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(DataTypes{key_type_, value_type_}, Names{"keys", "values"})))
{
    assertKeyType();
}

void DataTypeMapV2::assertKeyType() const
{
    if (!key_type->isValueRepresentedByInteger() && !isStringOrFixedString(*key_type) && !WhichDataType(key_type).isNothing()
        && !WhichDataType(key_type).isUUID())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Type of Map key must be a type, that can be represented by integer or string or UUID,"
            " but {} given",
            key_type->getName());
}


std::string DataTypeMapV2::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "MapV2(" << key_type->getName() << ", " << value_type->getName() << ")";

    return s.str();
}

MutableColumnPtr DataTypeMapV2::createColumn() const
{
    return ColumnMapV2::create(nested->createColumn());
}

Field DataTypeMapV2::getDefault() const
{
    return Map();
}

SerializationPtr DataTypeMapV2::doGetDefaultSerialization() const
{
    return std::make_shared<SerializationMapV2>(
        key_type->getDefaultSerialization(), value_type->getDefaultSerialization(), nested->getDefaultSerialization());
}

bool DataTypeMapV2::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMapV2 & rhs_map = static_cast<const DataTypeMapV2 &>(rhs);
    return nested->equals(*rhs_map.nested);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception("Map data type family must have two arguments: key and value types", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

    DataTypes nested_types;
    nested_types.reserve(arguments->children.size());

    for (const ASTPtr & child : arguments->children)
        nested_types.emplace_back(DataTypeFactory::instance().get(child));

    return std::make_shared<DataTypeMapV2>(nested_types);
}


void registerDataTypeMapV2(DataTypeFactory & factory)
{
    factory.registerDataType("MapV2", create);
}
}
