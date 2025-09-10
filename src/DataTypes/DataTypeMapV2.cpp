#include <Columns/ColumnMapV2.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMapV2.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/Serializations/SerializationMapV2.h>
#include <DataTypes/Serializations/SerializationTuple.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Parsers/IAST.h>
#include <base/map.h>
#include <Common/StringUtils.h>


namespace DB
{

namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int BAD_ARGUMENTS;
}

DataTypeMapV2::DataTypeMapV2(const DataTypePtr & nested_) : nested(nested_)
{
    const auto * type_array = typeid_cast<const DataTypeArray *>(nested.get());
    if (!type_array)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    const auto * type_tuple = typeid_cast<const DataTypeTuple *>(type_array->getNestedType().get());
    if (!type_tuple)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    if (type_tuple->getElements().size() != 2)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected Array(Tuple(key, value)) type, got {}", nested->getName());

    key_type = type_tuple->getElement(0);
    value_type = type_tuple->getElement(1);
    assertKeyType();
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
    if (!isValidKeyType(key_type))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "MapV2 cannot have a key of type {}", key_type->getName());
}


std::string DataTypeMapV2::doGetName() const
{
    WriteBufferFromOwnString s;
    s << "MapV2(" << key_type->getName() << ", " << value_type->getName() << ")";

    return s.str();
}

std::string DataTypeMapV2::doGetPrettyName(size_t indent) const
{
    WriteBufferFromOwnString s;
    s << "MapV2(" << key_type->getPrettyName(indent) << ", " << value_type->getPrettyName(indent) << ')';
    return s.str();
}

MutableColumnPtr DataTypeMapV2::createColumn() const
{
    return ColumnMapV2::create(nested->createColumn());
}

Field DataTypeMapV2::getDefault() const
{
    return MapV2();
}

SerializationPtr DataTypeMapV2::doGetDefaultSerialization() const
{
    auto key_serialization = key_type->getDefaultSerialization();
    auto value_serialization = value_type->getDefaultSerialization();
    /// Don't use nested->getDefaultSerialization() to avoid creating exponentially growing number of serializations for deep nested maps.
    /// Instead, reuse already created serializations for keys and values.
    auto key_serialization_named = std::make_shared<SerializationNamed>(key_serialization, "keys", SubstreamType::TupleElement);
    auto value_serialization_named = std::make_shared<SerializationNamed>(value_serialization, "values", SubstreamType::TupleElement);
    auto nested_serialization = std::make_shared<SerializationArray>(std::make_shared<SerializationTuple>(
        SerializationTuple::ElementSerializations{key_serialization_named, value_serialization_named}, true));
    return std::make_shared<SerializationMapV2>(key_serialization, value_serialization, nested_serialization);
}

bool DataTypeMapV2::equals(const IDataType & rhs) const
{
    if (typeid(rhs) != typeid(*this))
        return false;

    const DataTypeMapV2 & rhs_map = static_cast<const DataTypeMapV2 &>(rhs);
    return nested->equals(*rhs_map.nested);
}

bool DataTypeMapV2::isValidKeyType(DataTypePtr key_type)
{
    return isStringOrFixedString(key_type);
}

DataTypePtr DataTypeMapV2::getNestedTypeWithUnnamedTuple() const
{
    const auto & from_array = assert_cast<const DataTypeArray &>(*nested);
    const auto & from_tuple = assert_cast<const DataTypeTuple &>(*from_array.getNestedType());
    return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(from_tuple.getElements()));
}

void DataTypeMapV2::forEachChild(const DB::IDataType::ChildCallback & callback) const
{
    callback(*key_type);
    key_type->forEachChild(callback);
    callback(*value_type);
    value_type->forEachChild(callback);
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() != 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "MapV2 data type family must have two arguments: key and value types");

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
