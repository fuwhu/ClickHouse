#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnMapV2.h>
#include <Columns/IColumnImpl.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Common/WeakHash.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>
#include "Columns/ColumnNullable.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int NOT_IMPLEMENTED;
    extern const int LOGICAL_ERROR;
}


std::string ColumnMapV2::getName() const
{
    WriteBufferFromOwnString res;
    const auto & nested_tuple = getNestedData();
    res << "MapV2(" << nested_tuple.getColumn(0).getName() << ", " << nested_tuple.getColumn(1).getName() << ")";

    return res.str();
}

void ColumnMapV2::setColumns(const ColumnsWithTypeAndName & new_columns)
{
    columns.clear();
    columns = new_columns;
}

ColumnMapV2::ColumnMapV2(MutableColumnPtr && nested_) : nested(std::move(nested_))
{
    const auto * column_array = typeid_cast<const ColumnArray *>(nested.get());
    if (!column_array)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMapV2 can be created only from array of tuples");

    const auto * column_tuple = typeid_cast<const ColumnTuple *>(column_array->getDataPtr().get());
    if (!column_tuple)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMapV2 can be created only from array of tuples");

    if (column_tuple->getColumns().size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnMapV2 should contain only 2 subcolumns: keys and values");

    for (const auto & column : column_tuple->getColumns())
        if (isColumnConst(*column))
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "ColumnMapV2 cannot have ColumnConst as its element");
}

MutableColumnPtr ColumnMapV2::cloneEmpty() const
{
    return ColumnMapV2::create(nested->cloneEmpty());
}

MutableColumnPtr ColumnMapV2::cloneResized(size_t new_size) const
{
    return ColumnMapV2::create(nested->cloneResized(new_size));
}

Field ColumnMapV2::operator[](size_t n) const
{
    auto array = DB::get<Array>((*nested)[n]);
    return MapV2(std::make_move_iterator(array.begin()), std::make_move_iterator(array.end()));
}

void ColumnMapV2::get(size_t n, Field & res) const
{
    const auto & offsets = getNestedColumn().getOffsets();
    size_t offset = offsets[n - 1];
    size_t size = offsets[n] - offsets[n - 1];

    res = MapV2(size);
    auto & map = DB::get<MapV2 &>(res);

    for (size_t i = 0; i < size; ++i)
        getNestedData().get(offset + i, map[i]);
}

StringRef ColumnMapV2::getDataAt(size_t) const
{
    throw Exception("Method getDataAt is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

bool ColumnMapV2::isDefaultAt(size_t n) const
{
    return nested->isDefaultAt(n);
}

void ColumnMapV2::insertData(const char *, size_t)
{
    throw Exception("Method insertData is not supported for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
}

void ColumnMapV2::insert(const Field & x)
{
    const auto & map = DB::get<const MapV2 &>(x);
    nested->insert(Array(map.begin(), map.end()));
}

void ColumnMapV2::insertDefault()
{
    nested->insertDefault();
}
void ColumnMapV2::popBack(size_t n)
{
    nested->popBack(n);
}

StringRef ColumnMapV2::serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const
{
    return nested->serializeValueIntoArena(n, arena, begin);
}

const char * ColumnMapV2::deserializeAndInsertFromArena(const char * pos)
{
    return nested->deserializeAndInsertFromArena(pos);
}

const char * ColumnMapV2::skipSerializedInArena(const char * pos) const
{
    return nested->skipSerializedInArena(pos);
}

void ColumnMapV2::updateHashWithValue(size_t n, SipHash & hash) const
{
    nested->updateHashWithValue(n, hash);
}

void ColumnMapV2::updateWeakHash32(WeakHash32 & hash) const
{
    nested->updateWeakHash32(hash);
}

void ColumnMapV2::updateHashFast(SipHash & hash) const
{
    nested->updateHashFast(hash);
}

void ColumnMapV2::insertRangeFrom(const IColumn & src, size_t start, size_t length)
{
    nested->insertRangeFrom(assert_cast<const ColumnMapV2 &>(src).getNestedColumn(), start, length);
}

ColumnPtr ColumnMapV2::filter(const Filter & filt, ssize_t result_size_hint) const
{
    auto filtered = nested->filter(filt, result_size_hint);
    return ColumnMapV2::create(filtered);
}

void ColumnMapV2::expand(const IColumn::Filter & mask, bool inverted)
{
    nested->expand(mask, inverted);
}

ColumnPtr ColumnMapV2::permute(const Permutation & perm, size_t limit) const
{
    auto permuted = nested->permute(perm, limit);
    return ColumnMapV2::create(std::move(permuted));
}

ColumnPtr ColumnMapV2::index(const IColumn & indexes, size_t limit) const
{
    auto res = nested->index(indexes, limit);
    return ColumnMapV2::create(std::move(res));
}

ColumnPtr ColumnMapV2::replicate(const Offsets & offsets) const
{
    auto replicated = nested->replicate(offsets);
    auto col_map_v2 = ColumnMapV2::create(std::move(replicated));

    if (columns.empty())
        return col_map_v2;

    col_map_v2->constructImplicitColumns();
    return col_map_v2;
}

MutableColumns ColumnMapV2::scatter(ColumnIndex num_columns, const Selector & selector) const
{
    auto scattered_columns = nested->scatter(num_columns, selector);
    MutableColumns res;
    res.reserve(num_columns);
    for (auto && scattered : scattered_columns)
        res.push_back(ColumnMapV2::create(std::move(scattered)));

    return res;
}

int ColumnMapV2::compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const
{
    const auto & rhs_map = assert_cast<const ColumnMapV2 &>(rhs);
    return nested->compareAt(n, m, rhs_map.getNestedColumn(), nan_direction_hint);
}

void ColumnMapV2::compareColumn(
    const IColumn & rhs,
    size_t rhs_row_num,
    PaddedPODArray<UInt64> * row_indexes,
    PaddedPODArray<Int8> & compare_results,
    int direction,
    int nan_direction_hint) const
{
    return doCompareColumn<ColumnMapV2>(
        assert_cast<const ColumnMapV2 &>(rhs), rhs_row_num, row_indexes, compare_results, direction, nan_direction_hint);
}

bool ColumnMapV2::hasEqualValues() const
{
    return hasEqualValuesImpl<ColumnMapV2>();
}

void ColumnMapV2::getPermutation(
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    IColumn::Permutation & res) const
{
    nested->getPermutation(direction, stability, limit, nan_direction_hint, res);
}

void ColumnMapV2::updatePermutation(
    IColumn::PermutationSortDirection direction,
    IColumn::PermutationSortStability stability,
    size_t limit,
    int nan_direction_hint,
    IColumn::Permutation & res,
    EqualRanges & equal_ranges) const
{
    nested->updatePermutation(direction, stability, limit, nan_direction_hint, res, equal_ranges);
}

void ColumnMapV2::gather(ColumnGathererStream & gatherer)
{
    gatherer.gather(*this);
}

void ColumnMapV2::reserve(size_t n)
{
    nested->reserve(n);
}

void ColumnMapV2::ensureOwnership()
{
    nested->ensureOwnership();
}

size_t ColumnMapV2::byteSize() const
{
    return nested->byteSize();
}

size_t ColumnMapV2::byteSizeAt(size_t n) const
{
    return nested->byteSizeAt(n);
}

size_t ColumnMapV2::allocatedBytes() const
{
    return nested->allocatedBytes();
}

void ColumnMapV2::protect()
{
    nested->protect();
}

void ColumnMapV2::getExtremes(Field & min, Field & max) const
{
    Field nested_min;
    Field nested_max;

    nested->getExtremes(nested_min, nested_max);

    /// Convert result Array fields to Map fields because client expect min and max field to have type Map

    Array nested_min_value = nested_min.get<Array>();
    Array nested_max_value = nested_max.get<Array>();

    Map map_min_value(nested_min_value.begin(), nested_min_value.end());
    Map map_max_value(nested_max_value.begin(), nested_max_value.end());

    min = std::move(map_min_value);
    max = std::move(map_max_value);
}

void ColumnMapV2::forEachSubcolumn(ColumnCallback callback)
{
    nested->forEachSubcolumn(callback);
}

void ColumnMapV2::forEachSubcolumnRecursively(ColumnCallback callback)
{
    callback(nested);
    nested->forEachSubcolumnRecursively(callback);
}

bool ColumnMapV2::structureEquals(const IColumn & rhs) const
{
    if (const auto * rhs_map = typeid_cast<const ColumnMapV2 *>(&rhs))
        return nested->structureEquals(*rhs_map->nested);
    return false;
}

double ColumnMapV2::getRatioOfDefaultRows(double sample_ratio) const
{
    return getRatioOfDefaultRowsImpl<ColumnMapV2>(sample_ratio);
}

void ColumnMapV2::getIndicesOfNonDefaultRows(Offsets & indices, size_t from, size_t limit) const
{
    return getIndicesOfNonDefaultRowsImpl<ColumnMapV2>(indices, from, limit);
}

ColumnPtr ColumnMapV2::compress() const
{
    auto compressed = nested->compress();
    return ColumnCompressed::create(
        size(), compressed->byteSize(), [compressed = std::move(compressed)] { return ColumnMapV2::create(compressed->decompress()); });
}

DataTypePtr ColumnMapV2::getBaseDataType(const TypeIndex & type_index)
{
    switch (type_index)
    {
        case TypeIndex::String:
            return std::make_shared<DataTypeString>();
        case TypeIndex::UInt8:
            return std::make_shared<DataTypeUInt8>();
        case TypeIndex::UInt16:
            return std::make_shared<DataTypeUInt16>();
        case TypeIndex::UInt32:
            return std::make_shared<DataTypeUInt32>();
        case TypeIndex::UInt64:
            return std::make_shared<DataTypeUInt64>();
        case TypeIndex::UInt128:
            return std::make_shared<DataTypeUInt128>();
        case TypeIndex::UInt256:
            return std::make_shared<DataTypeUInt256>();
        case TypeIndex::Int8:
            return std::make_shared<DataTypeInt8>();
        case TypeIndex::Int16:
            return std::make_shared<DataTypeInt16>();
        case TypeIndex::Int32:
            return std::make_shared<DataTypeInt32>();
        case TypeIndex::Int64:
            return std::make_shared<DataTypeInt64>();
        case TypeIndex::Int128:
            return std::make_shared<DataTypeInt128>();
        case TypeIndex::Int256:
            return std::make_shared<DataTypeInt256>();
        case TypeIndex::Float32:
            return std::make_shared<DataTypeFloat32>();
        case TypeIndex::Float64:
            return std::make_shared<DataTypeFloat64>();
        default:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported data type {} for value of MapV2.", type_index);
    }
}

void ColumnMapV2::constructImplicitColumns() const
{
    const auto & column_array = assert_cast<const ColumnArray &>(getNestedColumn());
    const auto & column_tuple = assert_cast<const ColumnTuple &>(column_array.getData());
    const auto & column_key = column_tuple.getColumn(0);
    const auto & column_value = column_tuple.getColumn(1);

    std::unordered_map<String, IColumn::MutablePtr> names_and_columns;
    DataTypePtr value_type;

    if (column_value.getDataType() == TypeIndex::Nullable)
    {
        const ColumnNullable & nullable_column_value = assert_cast<const ColumnNullable &>(column_value);
        DataTypePtr nested_type;
        nested_type = getBaseDataType(nullable_column_value.getNestedColumn().getDataType());
        value_type = std::make_shared<DataTypeNullable>(nested_type);
    }
    else if (column_value.getDataType() == TypeIndex::Array)
    {
        const ColumnArray & array_column_value = assert_cast<const ColumnArray &>(column_value);
        DataTypePtr nested_type;
        nested_type = getBaseDataType(array_column_value.getData().getDataType());
        value_type = std::make_shared<DataTypeArray>(nested_type);
    }
    else
        value_type = getBaseDataType(column_value.getDataType());

    size_t rows_size = size();
    const ColumnArray::Offsets & array_offsets = column_array.getOffsets();

    for (size_t row_num = 0; row_num < size(); ++row_num)
    {
        size_t elements_size = row_num == 0 ? array_offsets[0] : (array_offsets[row_num] - array_offsets[row_num - 1]);
        size_t ps = row_num == 0 ? 0 : array_offsets[row_num - 1];
        for (size_t element_num = 0; element_num < elements_size; ++element_num)
        {
            const auto & k = column_key.getDataAt(element_num + ps);
            const auto & v = column_value[element_num + ps];
            const auto & k_str = k.toString();
            if (k_str.find(IMPLICIT_DELIMITER) != std::string::npos || k_str.find('/') != std::string::npos
                || k_str.find('\\') != std::string::npos)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported {} for key of MapV2.", k_str);

            auto it = names_and_columns.find(k.toString());
            if (it == names_and_columns.end())
            {
                //generate a new column fill in with default value
                const auto & new_column = column_value.cloneEmpty();
                if (row_num != 0)
                    new_column->insertManyDefaults(row_num);
                new_column->insert(v);
                names_and_columns.insert({k.toString(), new_column->assumeMutable()});
            }
            else
            {
                auto & key_column = it->second;

                //if this column size less than max_element_size
                //we should fill it with default values
                //till the size of this column be the same
                // as the max column's.
                //FIXME: Here is a bug, if a map have two same map key, clickhouse client will be blocked ?
                if (row_num > key_column->size())
                    key_column->insertManyDefaults(row_num - key_column->size());
                else if (key_column->size() > row_num)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "size of implicit column for key {} is {}, it is bigger than mapV2 size {}, "
                        "which means some map may have a single key more than once.",
                        k_str,
                        key_column->size(),
                        row_num);
                //TODO: need to support more data type
                key_column->insert(v);
            }
        }
    }
    ColumnsWithTypeAndName implicit_columns;

    for (const auto & name_column : names_and_columns)
    {
        size_t default_size = rows_size - name_column.second->size();
        name_column.second->insertManyDefaults(default_size);
        ColumnWithTypeAndName column_to_map{name_column.second->getPtr(), value_type, name_column.first};
        implicit_columns.emplace_back(column_to_map);
    }
    columns = implicit_columns;
}
}
