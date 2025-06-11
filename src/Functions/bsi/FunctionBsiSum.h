#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <base/logger_useful.h>
#include <Common/typeid_cast.h>
#include "Columns/ColumnTuple.h"
#include "Columns/IColumn.h"
#include "DataTypes/IDataType.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionBsiSum : public IFunction
{
public:
    static constexpr auto name = "bsi_sum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiSum>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeTuple>(DataTypes{std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()});
    }

    static void
    sum(const ColumnAggregateFunction & array_data,
        const ColumnArray::Offsets & array_offsets,
        ColumnPtr & result,
        const ColumnAggregateFunction * rbm_col)
    {
        size_t pos = 0;
        size_t row = 0;

        auto ret_sum = ColumnUInt64::create();
        auto ret_count = ColumnUInt64::create();

        for (const auto *offsets_data = array_offsets.data(), *end = offsets_data + array_offsets.size(); offsets_data < end;
             ++offsets_data)
        {
            auto offset_data = *offsets_data;
            size_t row_offset = 0;
            UInt64 sum_value = 0;
            UInt64 count_value = 0;

            for (; pos < offset_data; ++pos)
            {
                const auto & bitmap_data = *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(array_data.getData()[pos]);

                if (row_offset == 0)
                {
                    /// id bitmap
                    if (rbm_col)
                    {
                        const AggregateFunctionGroupBitmapData<UInt64> & rbm_data = rbm_col->size() == 1
                            ? *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(rbm_col->getData()[0])
                            : *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(rbm_col->getData()[row]);

                        count_value = bitmap_data.rbs.rb_and_cardinality(rbm_data.rbs);
                    }
                    else
                        count_value = bitmap_data.rbs.size();
                }
                else if (row_offset > 0)
                {
                    /// bsi slices
                    if (rbm_col)
                    {
                        const AggregateFunctionGroupBitmapData<UInt64> & rbm_data = rbm_col->size() == 1
                            ? *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(rbm_col->getData()[0])
                            : *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(rbm_col->getData()[row]);

                        UInt64 rb_and_cardinality = bitmap_data.rbs.rb_and_cardinality(rbm_data.rbs);
                        sum_value += rb_and_cardinality * std::pow(2, row_offset - 1);
                    }
                    else
                    {
                        UInt64 rb_cardinality = bitmap_data.rbs.size();
                        sum_value += rb_cardinality * std::pow(2, row_offset - 1);
                    }
                }

                row_offset++;
            }

            ret_sum->insert(sum_value);
            ret_count->insert(count_value);

            row++;
        }

        result = ColumnTuple::create(Columns{std::move(ret_sum), std::move(ret_count)});
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        size_t num_args = arguments.size();

        if (num_args < 1)
            throw Exception("Function " + getName() + " requires at least one arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (num_args > 2)
            throw Exception("Function " + getName() + " requires at most two parameters.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const ColumnArray & array = typeid_cast<const ColumnArray &>(*arguments[0].column);
        const ColumnArray::Offsets & array_offsets = array.getOffsets();
        const ColumnAggregateFunction & array_data = typeid_cast<const ColumnAggregateFunction &>(array.getData());

        const ColumnAggregateFunction * rbm_col = nullptr;

        if (num_args == 2)
        {
            if (isColumnConst(*arguments[1].column))
                rbm_col
                    = &typeid_cast<const ColumnAggregateFunction &>(typeid_cast<const ColumnConst &>(*arguments[1].column).getDataColumn());
            else
                rbm_col = &typeid_cast<const ColumnAggregateFunction &>(*arguments[1].column);

            const auto & aggregate_function = rbm_col->getAggregateFunction();
            const auto & data_type = aggregate_function->getArgumentTypes()[0];

            if (!WhichDataType(data_type).isUInt64()) {
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                              "Function {} only supports UInt64 bitmap types.",
                              getName());
            }
        }

        ColumnPtr result;
        sum(array_data, array_offsets, result, rbm_col);
        return result;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

}
