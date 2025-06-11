#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/AggregateFunctionGroupBitmapData.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeCustom.h>
#include <DataTypes/DataTypeCustomBSI.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <base/logger_useful.h>
#include <Common/typeid_cast.h>
#include <cstddef>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionBsiProductSum : public IFunction
{
public:
    static constexpr auto name = "bsi_product_sum";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiProductSum>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

private:
    static const AggregateFunctionGroupBitmapData<UInt64> & getBitmapDataRefOfBSI(
        const ColumnAggregateFunction & col, size_t pos)
    {
        return *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(col.getData()[pos]);
    }

    static void productSum(
        const ColumnAggregateFunction & array_data_i,
        const ColumnArray::Offsets & array_offsets_i,
        const ColumnAggregateFunction & array_data_j,
        const ColumnArray::Offsets & array_offsets_j,
        ColumnUInt64::Container & result_data)
    {
        size_t current_i_pos = 0;
        size_t current_j_pos = 0;

        /// Reduce the product creations of bitmap and lower memory consumption.
        AggregateFunctionGroupBitmapData<UInt64> not_null_tmp_rbm;
        AggregateFunctionGroupBitmapData<UInt64> tmp_rbm;
        AggregateFunctionGroupBitmapData<UInt64> empty_rbm;
    
        for (size_t row = 0; row < array_offsets_i.size(); ++row)
        {
            size_t total_sum = 0;

            size_t i_end = array_offsets_i[row];
            size_t row_start_i = current_i_pos;
    
            size_t j_end;
            if (array_offsets_j.size() == 1) 
            {
                j_end = array_offsets_j[0];
                current_j_pos = 0;
            }
            else
                j_end = array_offsets_j[row];

            size_t row_start_j = current_j_pos;
    
            /// If bitmap is not empty, clear it.
            if (not_null_tmp_rbm.rbs.size() != 0)
                not_null_tmp_rbm.rbs.rb_and(empty_rbm.rbs);
            not_null_tmp_rbm.rbs.merge(getBitmapDataRefOfBSI(array_data_i, current_i_pos).rbs);
            not_null_tmp_rbm.rbs.rb_and(getBitmapDataRefOfBSI(array_data_j, current_j_pos).rbs);
    
            if (not_null_tmp_rbm.rbs.size() > 0)
            {
                for (current_i_pos = row_start_i + 1; current_i_pos < i_end; ++current_i_pos)
                {
                    for (current_j_pos = row_start_j + 1; current_j_pos < j_end; ++current_j_pos)
                    {
                        /// If bitmap is not empty, clear it.
                        if (tmp_rbm.rbs.size() != 0)
                            tmp_rbm.rbs.rb_and(empty_rbm.rbs);
                        tmp_rbm.rbs.merge(getBitmapDataRefOfBSI(array_data_i, current_i_pos).rbs);
                        tmp_rbm.rbs.rb_and(getBitmapDataRefOfBSI(array_data_j, current_j_pos).rbs);

                        if (tmp_rbm.rbs.size() == 0)
                            continue;
    
                        size_t k = current_i_pos - row_start_i - 1;
                        size_t l = current_j_pos - row_start_j - 1;
                        size_t sum_k_l = tmp_rbm.rbs.size() * (1 << (k + l));
                        total_sum += sum_k_l;
                    }
                }
            }
            else
            {
                current_i_pos = i_end;
                current_j_pos = j_end;
            }

            result_data.push_back(total_sum);
        }
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & result_type,
        size_t input_rows_count) const override
    {
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        if (arguments.size() != 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} expects 2 arguments actual arguments {}",
                getName(), arguments.size());

        const auto & array_i = typeid_cast<const ColumnArray &>(*arguments[0].column);
        const auto & array_offsets_i = array_i.getOffsets();
        const auto & array_data_i = typeid_cast<const ColumnAggregateFunction &>(array_i.getData());

        const ColumnArray & array_j = isColumnConst(*arguments[1].column)
            ? typeid_cast<const ColumnArray &>(typeid_cast<const ColumnConst &>(*arguments[1].column).getDataColumn())
            : typeid_cast<const ColumnArray &>(*arguments[1].column);
        const auto & array_offsets_j = array_j.getOffsets();
        const auto & array_data_j = typeid_cast<const ColumnAggregateFunction &>(array_j.getData());

        auto result_column = ColumnUInt64::create();
        result_column->reserve(input_rows_count);
        auto & result_data = result_column->getData();
        productSum(array_data_i, array_offsets_i, array_data_j, array_offsets_j, result_data);

        return result_column;
    }
};

}
