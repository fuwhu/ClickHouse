#include "Processors/Transforms/ZCurveDictionaryTransform.h"

#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Storages/MergeTree/MergeTreeDictionary.h>
#include <base/range.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ZCurveDictionaryTransform::ZCurveDictionaryTransform(
    const Block & header_, const std::vector<String> & z_curve_columns_, const MergeTreeDictionaryStore & dict_store_, ContextPtr context_)
    : ISimpleTransform(header_, transformHeader(header_), false)
    , z_curve_columns(z_curve_columns_)
    , dict_store(dict_store_)
    , context(context_)
{
}

Block ZCurveDictionaryTransform::transformHeader(const Block & header)
{
    Block res;

    for (const auto & column : header)
        res.insert(column);

    auto type = std::make_shared<DataTypeUInt64>();
    res.insert(ColumnWithTypeAndName{type->createColumn(), type, "_zcurve"});

    return res;
}

void ZCurveDictionaryTransform::transform(Chunk & chunk)
{
    auto num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    ColumnsWithTypeAndName z_curve_args;

    const auto & header = input.getHeader();
    for (auto i : collections::range(z_curve_columns.size()))
    {
        auto column_name = z_curve_columns[i];
        auto & col = columns[header.getPositionByName(column_name)];
        auto dict = dict_store.getDictionary(column_name);
        if (!dict)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find MergeTreeDictionary for column {}", column_name);

        auto index_column = ColumnUInt64::create(num_rows);
        auto & container = index_column->getData();
        for (auto n : collections::range(num_rows))
        {
            auto idx = dict->getIndex(col->getDataAt(n));
            if (!idx)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not get index for col: {}", column_name);

            container[n] = *idx;
        }

        z_curve_args.emplace_back(ColumnWithTypeAndName{std::move(index_column), std::make_shared<DataTypeUInt64>(), ""});
    }

    auto func = FunctionFactory::instance().get("zCurve", context);
    auto z_value_col = func->build(z_curve_args)->execute(z_curve_args, std::make_shared<DataTypeUInt64>(), num_rows);

    columns.emplace_back(std::move(z_value_col));

    chunk.setColumns(std::move(columns), num_rows);
}
}
