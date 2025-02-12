#include <memory>
#include <utility>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <base/types.h>
#include <Common/PODArray_fwd.h>
#include <Common/typeid_cast.h>

namespace DB
{

class FunctionBsiTopK : public IFunction
{
public:
    static constexpr auto name = "bsi_topk";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiTopK>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeArray>(arguments[0].get()) == nullptr)
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First argument column should be BSI");
        }

        WhichDataType which(arguments[1].get());

        if (!which.isNativeUInt())
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second argument column should be UInt, but it has type {}", arguments[1]->getName());
        }
        DataTypes data_types = {std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()};

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeTuple>(data_types));
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        bool is_const_column;
        const IColumn * col_ptr;
        const PaddedPODArray<UInt64> * container;

        auto col_uint64 = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
        col_ptr = col_uint64.get();
        is_const_column = isColumnConst(*col_ptr);

        if (is_const_column)
            container = &typeid_cast<const ColumnUInt64 *>(typeid_cast<const ColumnConst *>(col_ptr)->getDataColumnPtr().get())->getData();
        else
            container = &typeid_cast<const ColumnUInt64 *>(col_ptr)->getData();

        const ColumnArray * col_arr = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        const ColumnArray::Offsets & offsets = col_arr->getOffsets();
        size_t offsets_size = offsets.size();

        const ColumnAggregateFunction * col_nested = typeid_cast<const ColumnAggregateFunction *>(col_arr->getDataPtr().get());
        const auto * offset_data = offsets.data();

        ///output data
        Columns cols = {ColumnUInt64::create(), ColumnUInt64::create()};
        auto col_tuple = ColumnTuple::create(cols);
        auto col_arr_res = ColumnArray::create(col_tuple);

        size_t pos = 0;
        size_t row = 0;

        for (const auto * end = offset_data + offsets_size; offset_data < end; ++offset_data)
        {
            UInt64 data = is_const_column ? (*container)[0] : (*container)[row];

            size_t row_size = *offset_data;

            AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence
                = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested->getData()[pos]);
            pos++;

            size_t slice_number = row_size - pos;
            size_t row_start = pos;

            if (data > bitmap_existence.roaring_bitmap_with_small_set.size())
            {
                /// if k of topk is greater than size of existence bitmap, return all metrics
                PaddedPODArray<UInt64> id_arr;
                bitmap_existence.roaring_bitmap_with_small_set.rb_to_array(id_arr);
                Array field;
                getArrayField(id_arr, field, slice_number, col_nested, row_start);
                col_arr_res->assumeMutable()->insert(field);

                pos += slice_number;
                continue;
            }

            AggregateFunctionGroupBitmapData<UInt64> bitmap_res;


            for (int i = static_cast<int>(slice_number) - 1; i >= 0; --i)
            {
                AggregateFunctionGroupBitmapData<UInt64> & bitmap_data
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested->getData()[row_start + i]);
                AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp;

                bitmap_tmp.roaring_bitmap_with_small_set.rb_or(bitmap_existence.roaring_bitmap_with_small_set);
                bitmap_tmp.roaring_bitmap_with_small_set.rb_and(bitmap_data.roaring_bitmap_with_small_set);
                size_t count = bitmap_tmp.roaring_bitmap_with_small_set.rb_or_cardinality(bitmap_res.roaring_bitmap_with_small_set);

                if (count > data)
                {
                    bitmap_existence.roaring_bitmap_with_small_set.rb_and(bitmap_data.roaring_bitmap_with_small_set);
                }
                else if (count < data)
                {
                    bitmap_res.roaring_bitmap_with_small_set.rb_or(bitmap_tmp.roaring_bitmap_with_small_set);
                    bitmap_existence.roaring_bitmap_with_small_set.rb_andnot(bitmap_data.roaring_bitmap_with_small_set);
                }
                else
                {
                    bitmap_existence.roaring_bitmap_with_small_set.rb_and(bitmap_data.roaring_bitmap_with_small_set);
                    pos = row_size;
                    break;
                }

                ++pos;
            }

            size_t count = bitmap_res.roaring_bitmap_with_small_set.rb_or_cardinality(bitmap_existence.roaring_bitmap_with_small_set);
            if (count > data)
            {
                AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp;
                bitmap_existence.roaring_bitmap_with_small_set.rb_limit(
                    0, data - bitmap_res.roaring_bitmap_with_small_set.size(), bitmap_tmp.roaring_bitmap_with_small_set);
                bitmap_res.roaring_bitmap_with_small_set.rb_or(bitmap_tmp.roaring_bitmap_with_small_set);
            }
            else
                bitmap_res.roaring_bitmap_with_small_set.rb_or(bitmap_existence.roaring_bitmap_with_small_set);


            /// get the metric value and make tuple of id and metric, return array of tuple
            PaddedPODArray<UInt64> id_arr;
            bitmap_res.roaring_bitmap_with_small_set.rb_to_array(id_arr);
            Array field;
            getArrayField(id_arr, field, slice_number, col_nested, row_start);
            col_arr_res->assumeMutable()->insert(field);
            ++row;
        }

        return std::move(col_arr_res);
    }

private:
    void getArrayField(
        PaddedPODArray<UInt64> & id_arr,
        Array & field,
        size_t slice_number,
        const ColumnAggregateFunction * col_nested,
        size_t row_start) const
    {
        for (size_t i = 0; i < id_arr.size(); ++i)
        {
            UInt64 metrics = 0;
            for (int j = static_cast<int>(slice_number) - 1; j >= 0; --j)
            {
                AggregateFunctionGroupBitmapData<UInt64> & bitmap_slice
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested->getData()[row_start + j]);
                if (bitmap_slice.roaring_bitmap_with_small_set.rb_contains(id_arr[i]))
                {
                    metrics += static_cast<UInt64>(std::pow(2, j));
                }
            }
            Field f1 = id_arr[i];
            Field f2 = metrics;
            Tuple tuple{f1, f2};
            field.push_back(tuple);
        }
    }
};

REGISTER_FUNCTION(BsiTopK)
{
    factory.registerFunction<FunctionBsiTopK>();
}
}
