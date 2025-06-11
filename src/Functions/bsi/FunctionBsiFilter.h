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


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


class FunctionBsiFilter : public IFunction
{
public:
    static constexpr auto name = "bsi_filter";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiFilter>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /*arguments*/) const override
    {
        auto custom_type = std::make_unique<DataTypeCustomDesc>(std::make_unique<DataTypeCustomBSIName>());
        return DataTypeFactory::instance().getCustom(std::move(custom_type));
    }

    static void filter(
        const ColumnAggregateFunction & array_data,
        const ColumnArray::Offsets & array_offsets,
        MutableColumnPtr & result,
        const ColumnAggregateFunction & rbm_col)
    {
        DataTypes bitmap_argument_types = {std::make_shared<DataTypeUInt64>()};
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function = AggregateFunctionFactory::instance().get(
            AggregateFunctionGroupBitmapData<UInt64>::name(), bitmap_argument_types, params_row, properties);
        auto result_arr_data = ColumnAggregateFunction::create(bitmap_function);
        auto result_arr_offsets = ColumnUInt64::create();

        result_arr_data->reserve(array_data.size());

        size_t pos = 0;
        size_t row = 0;

        for (const auto *offsets_data = array_offsets.data(), *end = offsets_data + array_offsets.size(); offsets_data < end;
             ++offsets_data)
        {
            auto offset_data = *offsets_data;

            for (; pos < offset_data; ++pos)
            {
                result_arr_data->insertDefault();

                AggregateFunctionGroupBitmapData<DB::UInt64> & bitmap_data
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(array_data.getData()[pos]);
                AggregateFunctionGroupBitmapData<DB::UInt64> & result_bitmap_data
                    = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(result_arr_data->getData()[pos]);

                result_bitmap_data.rbs.merge(bitmap_data.rbs);

                const AggregateFunctionGroupBitmapData<UInt64> & rbm_data = rbm_col.size() == 1
                    ? *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(rbm_col.getData()[0])
                    : *reinterpret_cast<const AggregateFunctionGroupBitmapData<UInt64> *>(rbm_col.getData()[row]);

                result_bitmap_data.rbs.rb_and(rbm_data.rbs);
            }

            row++;

            result_arr_offsets->insert(result_arr_data->size());
        }

        result = ColumnArray::create(std::move(result_arr_data), std::move(result_arr_offsets));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (result_type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        size_t num_args = arguments.size();

        if (num_args != 2)
            throw Exception("Function " + getName() + " requires 2 arguments: bsi, filter_bitmap.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        const ColumnArray & array = typeid_cast<const ColumnArray &>(*arguments[0].column);
        const ColumnArray::Offsets & array_offsets = array.getOffsets();
        const ColumnAggregateFunction & array_data = typeid_cast<const ColumnAggregateFunction &>(array.getData());

        const ColumnAggregateFunction & rbm_col = isColumnConst(*arguments[1].column)
            ? typeid_cast<const ColumnAggregateFunction &>(typeid_cast<const ColumnConst &>(*arguments[1].column).getDataColumn())
            : typeid_cast<const ColumnAggregateFunction &>(*arguments[1].column);
        
        const auto & aggregate_function = rbm_col.getAggregateFunction();
        const auto & data_type = aggregate_function->getArgumentTypes()[0];

        if (!WhichDataType(data_type).isUInt64()) {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                          "Function {} only supports UInt64 bitmap types.",
                          getName());
        }

        MutableColumnPtr result;
        filter(array_data, array_offsets, result, rbm_col);
        return std::move(result);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
};

}
