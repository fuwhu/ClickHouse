#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeCustomBSI.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <base/logger_useful.h>
#include <cassert>
#include <cstddef>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionBsiSquareSum : public IFunction
{
public:
    static constexpr auto name = "bsi_square_sum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiSquareSum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

private:
    static const AggregateFunctionGroupBitmapData<UInt64> & getBitmapDataRefOfBSI(
        const ColumnAggregateFunction & col, size_t pos)
    {
        return *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(col.getData()[pos]);
    }

    static void squareSum(
        const ColumnAggregateFunction & array_data,
        const ColumnArray::Offsets & array_offsets,
        ColumnUInt64::Container & result_data)
    {
        size_t current_pos = 0;

        /// Reduce the multiple creations of bitmap and lower memory consumption.
        AggregateFunctionGroupBitmapData<UInt64> tmp_rbm;
        AggregateFunctionGroupBitmapData<UInt64> empty_rbm;

        for (size_t row = 0; row < array_offsets.size(); ++row)
        {
            size_t offset_end = array_offsets[row];
            size_t row_start = current_pos;
            size_t total_sum = 0;

            for (size_t i = row_start + 1; i < offset_end; ++i)
            {
                size_t k = i - row_start - 1;
                const auto & rbm_data = getBitmapDataRefOfBSI(array_data, i);
                
                if (rbm_data.rbs.size() == 0)
                    continue;

                size_t sum_k_l = rbm_data.rbs.size() * (1ULL << (2 * k));
                total_sum += sum_k_l;
            }


            for (size_t i = row_start + 1; i < offset_end; ++i)
            {
                for (size_t j = i + 1; j < offset_end; ++j)
                {
                    /// If bitmap is not empty, clear it.
                    if (tmp_rbm.rbs.size() != 0)
                        tmp_rbm.rbs.rb_and(empty_rbm.rbs);

                    tmp_rbm.rbs.merge(getBitmapDataRefOfBSI(array_data, i).rbs);
                    tmp_rbm.rbs.rb_and(getBitmapDataRefOfBSI(array_data, j).rbs);

                    if (tmp_rbm.rbs.size() == 0)
                        continue;

                    size_t k = i - row_start - 1;
                    size_t l = j - row_start - 1;
                    size_t sum_k_l = tmp_rbm.rbs.size() * (1ULL << (k + l + 1));
                    total_sum += sum_k_l;
                }
            }

            result_data.push_back(total_sum);
            current_pos = offset_end;
        }
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const override
    {
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        if (arguments.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                          "Function {} expects 1 argument actual arguments {}",
                          getName(), arguments.size());

        const auto & array = typeid_cast<const ColumnArray &>(*arguments[0].column);
        const auto & array_data = typeid_cast<const ColumnAggregateFunction &>(array.getData());
        const auto & array_offsets = array.getOffsets();

        auto result_column = ColumnUInt64::create();
        result_column->reserve(input_rows_count);
        auto & result_data = result_column->getData();

        squareSum(array_data, array_offsets, result_data);

        return result_column;
    }
};

}
