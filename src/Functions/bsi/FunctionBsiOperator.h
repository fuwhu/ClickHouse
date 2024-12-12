#pragma once

#include <algorithm>
#include <cmath>
#include <memory>
#include <Poco/Logger.h>
#include "Common/Exception.h"
#include "Common/PODArray.h"
#include "Common/PODArray_fwd.h"
#include "Common/typeid_cast.h"
#include "AggregateFunctions/AggregateFunctionFactory.h"
#include "AggregateFunctions/AggregateFunctionGroupBitmapData.h"
#include "AggregateFunctions/IAggregateFunction.h"
#include "Columns/ColumnAggregateFunction.h"
#include "Columns/ColumnArray.h"
#include "Columns/ColumnConst.h"
#include "Columns/ColumnVector.h"
#include "Columns/ColumnsNumber.h"
#include "Columns/IColumn.h"
#include "Core/ColumnsWithTypeAndName.h"
#include "Core/Field.h"
#include "DataTypes/DataTypeAggregateFunction.h"
#include "DataTypes/DataTypeArray.h"
#include "DataTypes/DataTypeCustomBSI.h"
#include "DataTypes/DataTypeFactory.h"
#include "DataTypes/DataTypesNumber.h"
#include "DataTypes/IDataType.h"
#include "Functions/FunctionFactory.h"
#include "Functions/FunctionHelpers.h"
#include "Functions/FunctionsBitmap.h"
#include "Interpreters/Context_fwd.h"
#include "Interpreters/castColumn.h"
#include "base/logger_useful.h"
#include "base/types.h"

namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <typename Impl>
class FunctionBsiOperator : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiOperator<Impl>>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeArray>(arguments[0].get()) == nullptr)
        {
            throw Exception("First argument column should be BSI", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        WhichDataType which(arguments[1].get());

        if (!which.isNativeUInt())
        {
            throw Exception("Second argument column should be UInt, but it has type " +  arguments[1]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_func = AggregateFunctionFactory::instance().get(AggregateFunctionGroupBitmapData<UInt64>::name(), argument_types, params_row, properties);

        return std::make_shared<DataTypeAggregateFunction>(bitmap_func, argument_types, params_row);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        return executeBsiData(arguments, input_rows_count);
    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    ColumnPtr executeBsiData(const ColumnsWithTypeAndName & arguments, size_t /*input_row_count*/) const
    {
        /// input data
        bool is_column_const;
        const IColumn * column_ptr;

        const PaddedPODArray<UInt64> * container;
        auto col_uint64 = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
        column_ptr = col_uint64.get();
        is_column_const = isColumnConst(*column_ptr);

        if (is_column_const)
            container = &typeid_cast<const ColumnUInt64 *>(typeid_cast<const ColumnConst *>(column_ptr)->getDataColumnPtr().get())->getData();
        else 
            container = &typeid_cast<const ColumnUInt64 *>(column_ptr)->getData();

        const ColumnArray * col_arr = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        const ColumnAggregateFunction & col_nested = typeid_cast<const ColumnAggregateFunction &>(col_arr->getData());

        const ColumnArray::Offsets & offsets = col_arr->getOffsets();
        size_t offsets_size = offsets.size();
        const auto * offset_data = offsets.data();
        
        ///output data
        DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
        Array parameters;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function = AggregateFunctionFactory::instance().get(AggregateFunctionGroupBitmapData<UInt64>::name(), argument_types, parameters, properties);
        auto col_res = ColumnAggregateFunction::create(bitmap_function);

        size_t pos = 0;
        size_t row = 0;

        ///each row
        for (const auto * end = offset_data + offsets_size; offset_data < end; ++offset_data)
        {
            UInt64 data = is_column_const ? (*container)[0] : (*container)[row];

            col_res->insertDefault();
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_res = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_res->getData()[col_res->size() - 1]);

            size_t row_size = *offset_data;
            
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[pos]);
            
            pos++;
            size_t slice_number = row_size - pos;
            
            Impl::apply(data, bitmap_res, bitmap_existence, slice_number, col_nested, pos);

            ++row;
        }

        return std::move(col_res);


    }

};

struct BsiGtImpl
{
    static constexpr auto name = "bsi_gt";
    static void apply(UInt64 value, AggregateFunctionGroupBitmapData<UInt64> & bitmap_greater, AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence, 
                        size_t slice_number, const ColumnAggregateFunction & col_nested, size_t & pos)
    {
        if (value >> slice_number)
        {
            pos += slice_number;
            return;
        }

        size_t row_start = pos;
        ///each element except first notnull bitmap
        for (int i = slice_number - 1; i >= 0; --i)
        {
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_data = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[row_start + i]);
            
            bool exist = (value >> i) & 1;
            if (exist)
            {
                bitmap_existence.rbs.rb_and(bitmap_data.rbs);
            }
            else
            {
                AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp; 
                bitmap_tmp.rbs.rb_or(bitmap_existence.rbs);

                bitmap_tmp.rbs.rb_and(bitmap_data.rbs);
                bitmap_greater.rbs.rb_or(bitmap_tmp.rbs);
                bitmap_existence.rbs.rb_andnot(bitmap_data.rbs);
            }

            pos++;
        }
    }
};

struct BsiGeImpl
{
    static constexpr auto name = "bsi_ge";
    static void apply(UInt64 value, AggregateFunctionGroupBitmapData<UInt64> & bitmap_greater, AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence, 
                        size_t slice_number, const ColumnAggregateFunction & col_nested, size_t & pos)
    {
        if (value >> slice_number)
        {
            pos += slice_number;
            return;
        }

        size_t row_start = pos;
        ///each element except first notnull bitmap
        for (int i = slice_number - 1; i >= 0; --i)
        {
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_data = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[row_start + i]);
            
            bool exist = (value >> i) & 1;
            if (exist)
            {
                bitmap_existence.rbs.rb_and(bitmap_data.rbs);
            }
            else
            {
                AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp; 
                bitmap_tmp.rbs.rb_or(bitmap_existence.rbs);

                bitmap_tmp.rbs.rb_and(bitmap_data.rbs);
                bitmap_greater.rbs.rb_or(bitmap_tmp.rbs);
                bitmap_existence.rbs.rb_andnot(bitmap_data.rbs);
            }

            pos++;
        }

        bitmap_greater.rbs.rb_or(bitmap_existence.rbs);
    }
};

struct BsiLtImpl
{
    static constexpr auto name = "bsi_lt";
    static void apply(UInt64 value, AggregateFunctionGroupBitmapData<UInt64> & bitmap_less, AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence, 
                        size_t slice_number, const ColumnAggregateFunction & col_nested, size_t & pos)
    {
        if (value >> slice_number)
        {
            pos += slice_number;
            bitmap_less.rbs.rb_or(bitmap_existence.rbs);
            return;
        }

        size_t row_start = pos;
        ///each element except first notnull bitmap
        for (int i = slice_number - 1; i >= 0; --i)
        {
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_data = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[row_start + i]);
            
            bool exist = (value >> i) & 1;
            if (exist)
            {   
                AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp; 
                bitmap_tmp.rbs.rb_or(bitmap_existence.rbs);

                bitmap_tmp.rbs.rb_andnot(bitmap_data.rbs);
                bitmap_less.rbs.rb_or(bitmap_tmp.rbs);

                bitmap_existence.rbs.rb_and(bitmap_data.rbs);
            }
            else
            {
                bitmap_existence.rbs.rb_andnot(bitmap_data.rbs);
            }

            pos++;
        }

    }
};

struct BsiLeImpl
{
    static constexpr auto name = "bsi_le";
    static void apply(UInt64 value, AggregateFunctionGroupBitmapData<UInt64> & bitmap_less, AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence, 
                        size_t slice_number, const ColumnAggregateFunction & col_nested, size_t & pos)
    {
        if (value >> slice_number)
        {
            pos += slice_number;
            bitmap_less.rbs.rb_or(bitmap_existence.rbs);
            return;
        }

        size_t row_start = pos;
        ///each element except first notnull bitmap
        for (int i = slice_number - 1; i >= 0; --i)
        {
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_data = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[row_start + i]);
            
            bool exist = (value >> i) & 1;
            if (exist)
            {   
                AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp; 
                bitmap_tmp.rbs.rb_or(bitmap_existence.rbs);

                bitmap_tmp.rbs.rb_andnot(bitmap_data.rbs);
                bitmap_less.rbs.rb_or(bitmap_tmp.rbs);

                bitmap_existence.rbs.rb_and(bitmap_data.rbs);
            }
            else
            {
                bitmap_existence.rbs.rb_andnot(bitmap_data.rbs);
            }

            pos++;
        }
        bitmap_less.rbs.rb_or(bitmap_existence.rbs);
    }
};


using FunctionBsiGt = FunctionBsiOperator<BsiGtImpl>;
using FunctionBsiLt = FunctionBsiOperator<BsiLtImpl>;
using FunctionBsiGe = FunctionBsiOperator<BsiGeImpl>;
using FunctionBsiLe = FunctionBsiOperator<BsiLeImpl>;

class FunctionBsiRange : public IFunction
{
public:
    static constexpr auto name = "bsi_range";

    String getName() const override { return name; }

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionBsiRange>(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (checkAndGetDataType<DataTypeArray>(arguments[0].get()) == nullptr)
        {
            throw Exception("First argument column should be BSI", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        WhichDataType which1(arguments[1].get());
        WhichDataType which2(arguments[2].get());

        if (!which1.isNativeUInt())
        {
            throw Exception("Second argument column should be UInt, but it has type " +  arguments[1]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        if (!which2.isNativeUInt())
        {
            throw Exception("Third argument column should be UInt, but it has type " +  arguments[2]->getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
        Array params_row;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_func = AggregateFunctionFactory::instance().get(AggregateFunctionGroupBitmapData<UInt64>::name(), argument_types, params_row, properties);

        return std::make_shared<DataTypeAggregateFunction>(bitmap_func, argument_types, params_row);
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t input_rows_count) const override
    {
        
        return executeBsiData(arguments, input_rows_count);       

    }

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    static ColumnPtr executeBsiData(const ColumnsWithTypeAndName & arguments, size_t /*input_row_count*/) 
    {
        /// input data
        bool is_column_const[2];
        const IColumn * column_ptr[2];

        const PaddedPODArray<UInt64> * container0;
        const PaddedPODArray<UInt64> * container1;

        auto col_uint64_0 = castColumn(arguments[1], std::make_shared<DataTypeUInt64>());
        column_ptr[0] = col_uint64_0.get();
        is_column_const[0] = isColumnConst(*column_ptr[0]);

        auto col_uint64_1 = castColumn(arguments[2], std::make_shared<DataTypeUInt64>());
        column_ptr[1] = col_uint64_1.get();
        is_column_const[1] = isColumnConst(*column_ptr[1]);

        if (is_column_const[0])
            container0 = &typeid_cast<const ColumnUInt64 *>(typeid_cast<const ColumnConst *>(column_ptr[0])->getDataColumnPtr().get())->getData();
        else 
            container0 = &typeid_cast<const ColumnUInt64 *>(column_ptr[0])->getData();

        if (is_column_const[1])
            container1 = &typeid_cast<const ColumnUInt64 *>(typeid_cast<const ColumnConst *>(column_ptr[1])->getDataColumnPtr().get())->getData();
        else
            container1 = &typeid_cast<const ColumnUInt64 *>(column_ptr[1])->getData();
        

        const ColumnArray * col_arr = typeid_cast<const ColumnArray *>(arguments[0].column.get());
        const ColumnAggregateFunction & col_nested = typeid_cast<const ColumnAggregateFunction &>(col_arr->getData());

        const ColumnArray::Offsets & offsets = col_arr->getOffsets();
        size_t offsets_size = offsets.size();
        const auto * offset_data = offsets.data();
        
        ///output data
        DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
        Array parameters;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr bitmap_function = AggregateFunctionFactory::instance().get(AggregateFunctionGroupBitmapData<UInt64>::name(), argument_types, parameters, properties);
        auto col_res = ColumnAggregateFunction::create(bitmap_function);

        size_t pos = 0;
        size_t row = 0;

        ///each row
        for (const auto * end = offset_data + offsets_size; offset_data < end; ++offset_data)
        {
            UInt64 data1 = is_column_const[0] ? (*container0)[0] : (*container0)[row];
            UInt64 data2 = is_column_const[1] ? (*container1)[0] : (*container1)[row];

            col_res->insertDefault();
            

            AggregateFunctionGroupBitmapData<UInt64> & bitmap_res = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_res->getData()[col_res->size() - 1]);

            size_t row_size = *offset_data;
            
            AggregateFunctionGroupBitmapData<UInt64> & bitmap_existence = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[pos]);

            AggregateFunctionGroupBitmapData<UInt64> bitmap_existence_for_less;
            bitmap_existence_for_less.rbs.rb_or(bitmap_existence.rbs);

            pos++;

            size_t slice_number = row_size - pos;

            if (data1 > data2 || data1 >> slice_number)
            {
                pos = row_size;
                continue;
            }

            size_t row_start = pos;
            ///each element except first notnull bitmap
            for (int i = slice_number - 1; i >= 0; --i)
            {
                AggregateFunctionGroupBitmapData<UInt64> & bitmap_data = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[row_start + i]);
                
                bool exist = (data1 >> i) & 1;

                if (exist)
                {
                    bitmap_existence.rbs.rb_and(bitmap_data.rbs);
                }
                else
                {
                    AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp; 
                    bitmap_tmp.rbs.rb_or(bitmap_existence.rbs);

                    bitmap_tmp.rbs.rb_and(bitmap_data.rbs);
                    bitmap_res.rbs.rb_or(bitmap_tmp.rbs);
                    bitmap_existence.rbs.rb_andnot(bitmap_data.rbs);
                }

                pos++;
            }
            bitmap_res.rbs.rb_or(bitmap_existence.rbs);

            if (data2 >> slice_number == 0)
            {
                AggregateFunctionGroupBitmapData<UInt64> bitmap_res_for_less;
                for (int i = slice_number - 1; i >= 0; --i)
                {
                    AggregateFunctionGroupBitmapData<UInt64> & bitmap_data = *reinterpret_cast<AggregateFunctionGroupBitmapData<UInt64> *>(col_nested.getData()[row_start + i]);
                
                    bool exist = (data2 >> i) & 1;

                    if (exist)
                    {   
                        AggregateFunctionGroupBitmapData<UInt64> bitmap_tmp; 
                        bitmap_tmp.rbs.rb_or(bitmap_existence_for_less.rbs);

                        bitmap_tmp.rbs.rb_andnot(bitmap_data.rbs);
                        bitmap_res_for_less.rbs.rb_or(bitmap_tmp.rbs);
                        bitmap_existence_for_less.rbs.rb_and(bitmap_data.rbs);
                    }
                    else
                    {
                        bitmap_existence_for_less.rbs.rb_andnot(bitmap_data.rbs);
                    }  
                }
                bitmap_res_for_less.rbs.rb_or(bitmap_existence_for_less.rbs);
                bitmap_res.rbs.rb_and(bitmap_res_for_less.rbs);
            }

            

            ++row;
        }

        return std::move(col_res);


    }
    

};

}
