#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** MapV2 data type.
  * Same as Map but extract all keys and store them in different files
  */
class DataTypeMapV2 final : public IDataType
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;

    /// 'nested' is an Array(Tuple(key_type, value_type))
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    explicit DataTypeMapV2(const DataTypes & elems);
    DataTypeMapV2(const DataTypePtr & key_type_, const DataTypePtr & value_type_);

    TypeIndex getTypeId() const override { return TypeIndex::MapV2; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "MapV2"; }

    bool canBeInsideNullable() const override { return false; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
    bool isComparable() const override { return key_type->isComparable() && value_type->isComparable(); }
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }

    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }
    DataTypes getKeyValueTypes() const { return {key_type, value_type}; }
    const DataTypePtr & getNestedType() const { return nested; }

    SerializationPtr doGetDefaultSerialization() const override;

private:
    void assertKeyType() const;
};

}
