#pragma once

#include "IcebergCommon.h"

#include <optional>
#include <Core/SortDescription.h>
#include <Interpreters/Set.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/SelectQueryInfo.h>


namespace DB
{

class ASTFunction;
class Context;
class IFunction;
using FunctionBasePtr = std::shared_ptr<IFunctionBase>;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;


class IcebergKeyCondition
{
public:
    IcebergKeyCondition(
        const SelectQueryInfo & query_info,
        ContextPtr context,
        const Names & key_column_names,
        const ExpressionActionsPtr & key_expr,
        bool single_point_ = false,
        bool strict_ = false);

    using MonotonicFunctionsChain = std::vector<FunctionBasePtr>;

    static bool getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type);

    static Block getBlockWithConstants(const ASTPtr & query, const TreeRewriterResultPtr & syntax_analyzer_result, ContextPtr context);

private:
    struct RPNElement
    {
        RPNElement() = default;
        RPNElement(IcebergExpression::Type type_) : type(type_) { } /// NOLINT
        RPNElement(IcebergExpression::Type type_, size_t key_column_) : type(type_), key_column(key_column_) { }
        RPNElement(IcebergExpression::Type type_, size_t key_column_, const Field & value_)
            : type(type_), value(value_), key_column(key_column_)
        {
        }

        IcebergExpression::Type type = IcebergExpression::Type::UNKNOWN;

        Field value;
        size_t key_column = 0;

        MonotonicFunctionsChain monotonic_functions_chain;
    };

    using RPN = std::vector<RPNElement>;
    using ColumnIndices = std::map<String, size_t>;

    using AtomMap = std::unordered_map<std::string, bool (*)(RPNElement & out, const Field & value)>;

public:
    static const AtomMap atom_map;

    IcebergExpression::NodePtr getExpressionTree(bool skip_non_direct_column_ref = false) const;

private:
    void traverseAST(const ASTPtr & node, ContextPtr context, Block & block_with_constants);
    bool tryParseAtomFromAST(const ASTPtr & node, ContextPtr context, Block & block_with_constants, RPNElement & out);
    static bool tryParseLogicalOperatorFromAST(const ASTFunction * func, RPNElement & out);

    bool isKeyPossiblyWrappedByMonotonicFunctions(
        const ASTPtr & node,
        ContextPtr context,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain);

    bool isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        const ASTPtr & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        std::vector<const ASTFunction *> & out_functions_chain);

    bool transformConstantWithValidFunctions(
        const String & expr_name,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type,
        std::function<bool(IFunctionBase &, const IDataType &)> always_monotonic) const;

    bool canConstantBeWrappedByMonotonicFunctions(
        const ASTPtr & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type);

    bool canConstantBeWrappedByFunctions(
        const ASTPtr & ast, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type);

    RPN rpn;

    ColumnIndices key_columns;
    /// Expression which is used for key condition.
    const ExpressionActionsPtr key_expr;
    /// All intermediate columns are used to calculate key_expr.
    const NameSet key_subexpr_names;

    NameSet array_joined_columns;
    PreparedSets prepared_sets;

    // If true, always allow key_expr to be wrapped by function
    bool single_point;
    // If true, do not use always_monotonic information to transform constants
    bool strict;
};
}
