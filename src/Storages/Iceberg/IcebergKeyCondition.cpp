#include "IcebergKeyCondition.h"
#include "Storages/Iceberg/IcebergCommon.h"

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/Utils.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsConversion.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/Set.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <Interpreters/misc.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/KeyCondition.h>
#include <Common/FieldVisitorToString.h>
#include <Common/typeid_cast.h>
#include <Columns/ColumnSet.h>

#include <cassert>
#include <limits>
#include <stack>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_TYPE_OF_FIELD;
}

namespace Setting
{
    extern const SettingsDateTimeOverflowBehavior date_time_overflow_behavior;
}

namespace
{
    const std::map<std::string, std::string> inverse_relations = {
        {"equals", "notEquals"},
        {"notEquals", "equals"},
        {"less", "greaterOrEquals"},
        {"greaterOrEquals", "less"},
        {"greater", "lessOrEquals"},
        {"lessOrEquals", "greater"},
        {"in", "notIn"},
        {"notIn", "in"},
        {"globalIn", "globalNotIn"},
        {"globalNotIn", "globalIn"},
        {"nullIn", "notNullIn"},
        {"notNullIn", "nullIn"},
        {"globalNullIn", "globalNotNullIn"},
        {"globalNullNotIn", "globalNullIn"},
        {"isNull", "isNotNull"},
        {"isNotNull", "isNull"},
        {"like", "notLike"},
        {"notLike", "like"},
        {"empty", "notEmpty"},
        {"notEmpty", "empty"},
    };


    bool isLogicalOperator(const String & func_name)
    {
        return (func_name == "and" || func_name == "or" || func_name == "not" || func_name == "indexHint");
    }

    /// The node can be one of:
    ///   - Logical operator (AND, OR, NOT and indexHint() - logical NOOP)
    ///   - An "atom" (relational operator, constant, expression)
    ///   - A logical constant expression
    ///   - Any other function
    ASTPtr cloneASTWithInversionPushDown(const ASTPtr node, const bool need_inversion = false)
    {
        const ASTFunction * func = node->as<ASTFunction>();

        if (func && isLogicalOperator(func->name))
        {
            if (func->name == "not")
            {
                return cloneASTWithInversionPushDown(func->arguments->children.front(), !need_inversion);
            }

            const auto result_node = makeASTFunction(func->name);

            /// indexHint() is a special case - logical NOOP function
            if (result_node->name != "indexHint" && need_inversion)
            {
                result_node->name = (result_node->name == "and") ? "or" : "and";
            }

            if (func->arguments)
            {
                for (const auto & child : func->arguments->children)
                {
                    result_node->arguments->children.push_back(cloneASTWithInversionPushDown(child, need_inversion));
                }
            }

            return result_node;
        }

        auto cloned_node = node->clone();

        if (func && inverse_relations.find(func->name) != inverse_relations.cend())
        {
            if (need_inversion)
            {
                cloned_node->as<ASTFunction>()->name = inverse_relations.at(func->name);
            }

            return cloned_node;
        }

        return need_inversion ? makeASTFunction("not", cloned_node) : cloned_node;
    }
}

/// Functions with range inversion cannot be relaxed. It will become stricter instead.
/// For example:
/// create table test(d Date, k Int64, s String) Engine=MergeTree order by toYYYYMM(d);
/// insert into test values ('2020-01-01', 1, '');
/// insert into test values ('2020-01-02', 1, '');
/// select * from test where d != '2020-01-01'; -- If relaxed, no record will return
static const std::set<std::string_view> no_relaxed_atom_functions
    = {"notLike", "notIn", "globalNotIn", "notNullIn", "globalNotNullIn", "notEquals", "notEmpty"};

/// A dictionary containing actions to the corresponding functions to turn them into `RPNElement`
const IcebergKeyCondition::AtomMap IcebergKeyCondition::atom_map{
    {"notEquals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_EQ;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE;
         out.value = value;
         return true;
     }},
    {"equals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::EQ;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;
         return true;
     }},
    {"less",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::LT;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;
         return true;
     }},
    {"greater",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::GT;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;
         return true;
     }},
    {"lessOrEquals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::LT_EQ;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;
         return true;
     }},
    {"greaterOrEquals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::GT_EQ;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;
         return true;
     }},
    {"in",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_SET;
         out.value = value;
         return true;
     }},
    {"notIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
         out.value = value;
         return true;
     }},
    {"globalIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_SET;
         out.value = value;
         return true;
     }},
    {"globalNotIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
         out.value = value;
         return true;
     }},
    {"nullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_SET;
         out.value = value;
         return true;
     }},
    {"notNullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
         out.value = value;
         return true;
     }},
    {"globalNullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_SET;
         out.value = value;
         return true;
     }},
    {"globalNotNullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_SET;
         out.value = value;
         return true;
     }},
    {"empty",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::EQ;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = "";
         return true;
     }},
    {"notEmpty",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::NOT_EQ;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE;
         out.value = "";
         return true;
     }},
    {"like",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::LIKE;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;

         return true;
     }},
    {"notLike",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::NOT_LIKE;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_NOT_IN_RANGE;
         out.value = value;

         return true;
     }},
    {"startsWith",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::STARTS_WITH;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;

         return true;
     }},
    {"isNotNull",
     [](RPNElement & out, const Field &)
     {
         out.type = IcebergExpression::Type::NOT_NULL;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IS_NOT_NULL;
         // isNotNull means (-Inf, +Inf), which is the default Range
         return true;
     }},
    {"isNull",
     [](RPNElement & out, const Field &)
     {
         out.type = IcebergExpression::Type::IS_NULL;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IS_NULL;
         // isNull means +Inf (NULLS_LAST) or -Inf (NULLS_FIRST),
         // which is equivalent to not in Range (-Inf, +Inf)
         return true;
     }},
    {"hasToken",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::HAS_TERM;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;

         return true;
     }},
    {"has",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::ARRAY_CONTAINS;
         out.function = IcebergKeyCondition::RPNElement::FUNCTION_IN_RANGE;
         out.value = value;

         return true;
     }}};

/** Calculate expressions, that depend only on constants.
  * For index to work when something like "WHERE Date = toDate(now())" is written.
  */
Block IcebergKeyCondition::getBlockWithConstants(
    const ASTPtr & query, const TreeRewriterResultPtr & syntax_analyzer_result, ContextPtr context)
{
    Block result{{DataTypeUInt8().createColumnConstWithDefaultValue(1), std::make_shared<DataTypeUInt8>(), "_dummy"}};

    if (syntax_analyzer_result)
        // syntax_analyzer_result = TreeRewriter(context).analyze(query, )
    {
        const auto expr_for_constant_folding = ExpressionAnalyzer(query, syntax_analyzer_result, context).getConstActions();

        expr_for_constant_folding->execute(result);
    }

    return result;
}

static NameSet getAllSubexpressionNames(const ExpressionActions & key_expr)
{
    NameSet names;
    for (const auto & action : key_expr.getActions())
        names.insert(action.node->result_name);

    return names;
}

IcebergKeyCondition::IcebergKeyCondition(
    const ActionsDAGWithInversionPushDown & filter_dag,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    // , prepared_sets(query_info.sets)
    , single_point(single_point_)
    , strict(strict_)
    , date_time_overflow_behavior_ignore(
          context->getSettingsRef()[Setting::date_time_overflow_behavior] == FormatSettings::DateTimeOverflowBehavior::Ignore)
{
    size_t key_index = 0;
    for (const auto & name : key_column_names)
    {
        if (!key_columns.contains(name))
        {
            key_columns[name] = key_columns.size();
            key_indices.push_back(key_index);
        }
        ++key_index;
    }

    if (!filter_dag.predicate)
    {
        has_filter = false;
        relaxed = true;
        rpn.emplace_back(IcebergExpression::Type::UNKNOWN);
        return;
    }

    has_filter = true;

    RPNBuilder<RPNElement> builder(filter_dag.predicate, context, [&](const RPNBuilderTreeNode & node, RPNElement & out)
    {
        return extractAtomFromTree(node, out);
    });

    rpn = std::move(builder).extractRPN();

    // findHyperrectanglesForArgumentsOfSpaceFillingCurves();

    // if (std::any_of(rpn.begin(), rpn.end(), [&](const auto & elem) { return always_relaxed_atom_elements.contains(elem.function); }))
    //     relaxed = true;
}

IcebergKeyCondition::IcebergKeyCondition(
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    // , prepared_sets(query_info.sets)
    , single_point(single_point_)
    , strict(strict_)
{
    for (size_t i = 0, size = key_column_names.size(); i < size; ++i)
    {
        const auto & name = key_column_names[i];
        if (!key_columns.count(name))
            key_columns[name] = i;
    }

    /** Evaluation of expressions that depend only on constants.
      * For the index to be used, if it is written, for example `WHERE Date = toDate(now())`.
      */
    Block block_with_constants = getBlockWithConstants(query_info.query, query_info.syntax_analyzer_result, context);

    if (query_info.syntax_analyzer_result)
    {
        for (const auto & [name, _] : query_info.syntax_analyzer_result->array_join_result_to_source)
            array_joined_column_names.insert(name);
    }

    const ASTSelectQuery & select = query_info.query->as<ASTSelectQuery &>();
    if (select.where() || select.prewhere())
    {
        ASTPtr filter_query;
        if (select.where() && select.prewhere())
            filter_query = makeASTFunction("and", select.where(), select.prewhere());
        else
            filter_query = select.where() ? select.where() : select.prewhere();

        /** When non-strictly monotonic functions are employed in functional index (e.g. ORDER BY toStartOfHour(dateTime)),
          * the use of NOT operator in predicate will result in the indexing algorithm leave out some data.
          * This is caused by rewriting in IcebergCondition::tryParseAtomFromAST of relational operators to less strict
          * when parsing the AST into internal RPN representation.
          * To overcome the problem, before parsing the AST we transform it to its semantically equivalent form where all NOT's
          * are pushed down and applied (when possible) to leaf nodes.
          */
        traverseAST(cloneASTWithInversionPushDown(filter_query), context, block_with_constants);
    }
    else
    {
        rpn.emplace_back(IcebergExpression::Type::UNKNOWN);
    }
}

/** Computes value of constant expression and its data type.
  * Returns false, if expression isn't constant.
  */
bool IcebergKeyCondition::getConstant(const ASTPtr & expr, Block & block_with_constants, Field & out_value, DataTypePtr & out_type)
{
    // Constant expr should use alias names if any
    String column_name = expr->getColumnName();

    if (const auto * lit = expr->as<ASTLiteral>())
    {
        /// By default block_with_constants has only one column named "_dummy".
        /// If block contains only constants it's may not be preprocessed by
        //  ExpressionAnalyzer, so try to look up in the default column.
        if (!block_with_constants.has(column_name))
            column_name = "_dummy";

        /// Simple literal
        out_value = lit->value;
        out_type = block_with_constants.getByName(column_name).type;

        /// If constant is not Null, we can assume it's type is not Nullable as well.
        if (!out_value.isNull())
            out_type = removeNullable(out_type);

        return true;
    }
    else if (block_with_constants.has(column_name) && isColumnConst(*block_with_constants.getByName(column_name).column))
    {
        /// An expression which is dependent on constants only
        const auto & expr_info = block_with_constants.getByName(column_name);
        out_value = (*expr_info.column)[0];
        out_type = expr_info.type;

        if (!out_value.isNull())
            out_type = removeNullable(out_type);

        return true;
    }
    else
        return false;
}


DataTypePtr getArgumentTypeOfMonotonicFunction(const IFunctionBase & func);
bool applyFunctionChainToColumn(
    const ColumnPtr & in_column,
    const DataTypePtr & in_data_type,
    const std::vector<FunctionBasePtr> & functions,
    ColumnPtr & out_column,
    DataTypePtr & out_data_type);

/// The case when arguments may have types different than in the primary key.
static std::pair<Field, DataTypePtr>
applyFunctionForFieldOfUnknownType(const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value)
{
    ColumnsWithTypeAndName arguments{{arg_type->createColumnConst(1, arg_value), arg_type, "x"}};
    DataTypePtr return_type = func->getResultType();

    auto col = func->execute(arguments, return_type, 1, false);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}

/// Same as above but for binary operators
static std::pair<Field, DataTypePtr> applyBinaryFunctionForFieldOfUnknownType(
    const FunctionBasePtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value,
    const DataTypePtr & arg_type2,
    const Field & arg_value2)
{
    ColumnsWithTypeAndName arguments{
        {arg_type->createColumnConst(1, arg_value), arg_type, "x"}, {arg_type2->createColumnConst(1, arg_value2), arg_type2, "y"}};

    // FunctionBasePtr func_base = func->build(arguments);

    DataTypePtr return_type = func->getResultType();

    auto col = func->execute(arguments, return_type, 1, false);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}

void IcebergKeyCondition::traverseAST(const ASTPtr & node, ContextPtr context, Block & block_with_constants)
{
    RPNElement element;

    if (const auto * func = node->as<ASTFunction>())
    {
        if (tryParseLogicalOperatorFromAST(func, element))
        {
            auto & args = func->arguments->children;
            for (size_t i = 0, size = args.size(); i < size; ++i)
            {
                traverseAST(args[i], context, block_with_constants);

                /** The first part of the condition is for the correct support of `and` and `or` functions of arbitrary arity
                  * - in this case `n - 1` elements are added (where `n` is the number of arguments).
                  */
                if (i != 0 || element.type == IcebergExpression::Type::NOT)
                    rpn.emplace_back(element);
            }

            return;
        }
    }

    if (!tryParseAtomFromAST(node, context, block_with_constants, element))
    {
        element.type = IcebergExpression::Type::UNKNOWN;
    }

    rpn.emplace_back(std::move(element));
}


/** The key functional expression constraint may be inferred from a plain column in the expression.
  * For example, if the key contains `toStartOfHour(Timestamp)` and query contains `WHERE Timestamp >= now()`,
  * it can be assumed that if `toStartOfHour()` is monotonic on [now(), inf), the `toStartOfHour(Timestamp) >= toStartOfHour(now())`
  * condition also holds, so the index may be used to select only parts satisfying this condition.
  *
  * To check the assumption, we'd need to assert that the inverse function to this transformation is also monotonic, however the
  * inversion isn't exported (or even viable for not strictly monotonic functions such as `toStartOfHour()`).
  * Instead, we can qualify only functions that do not transform the range (for example rounding),
  * which while not strictly monotonic, are monotonic everywhere on the input range.
  */
bool IcebergKeyCondition::transformConstantWithValidFunctions(
    const String & expr_name,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type,
    std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const
{
    const auto & sample_block = key_expr->getSampleBlock();
    for (const auto & node : key_expr->getNodes())
    {
        auto it = key_columns.find(node.result_name);
        if (it != key_columns.end())
        {
            std::stack<const ActionsDAG::Node *> chain;

            const auto * cur_node = &node;
            bool is_valid_chain = true;

            while (is_valid_chain)
            {
                if (cur_node->result_name == expr_name)
                    break;

                chain.push(cur_node);

                if (cur_node->type == ActionsDAG::ActionType::FUNCTION && cur_node->children.size() <= 2)
                {
                    is_valid_chain = always_monotonic(*cur_node->function_base, *cur_node->result_type);

                    const ActionsDAG::Node * next_node = nullptr;
                    for (const auto * arg : cur_node->children)
                    {
                        if (arg->column && isColumnConst(*arg->column))
                            continue;

                        if (next_node)
                            is_valid_chain = false;

                        next_node = arg;
                    }

                    if (!next_node)
                        is_valid_chain = false;

                    cur_node = next_node;
                }
                else if (cur_node->type == ActionsDAG::ActionType::ALIAS)
                    cur_node = cur_node->children.front();
                else
                    is_valid_chain = false;
            }

            if (is_valid_chain)
            {
                /// Here we cast constant to the input type.
                /// It is not clear, why this works in general.
                /// I can imagine the case when expression like `column < const` is legal,
                /// but `type(column)` and `type(const)` are of different types,
                /// and const cannot be casted to column type.
                /// (There could be `superType(type(column), type(const))` which is used for comparison).
                ///
                /// However, looks like this case newer happenes (I could not find such).
                /// Let's assume that any two comparable types are castable to each other.
                auto const_type = cur_node->result_type;
                auto const_column = out_type->createColumnConst(1, out_value);
                auto const_value = (*castColumn({const_column, out_type, ""}, const_type))[0];

                while (!chain.empty())
                {
                    const auto * func = chain.top();
                    chain.pop();

                    if (func->type != ActionsDAG::ActionType::FUNCTION)
                        continue;

                    if (func->children.size() == 1)
                    {
                        std::tie(const_value, const_type)
                            = applyFunctionForFieldOfUnknownType(func->function_base, const_type, const_value);
                    }
                    else if (func->children.size() == 2)
                    {
                        const auto * left = func->children[0];
                        const auto * right = func->children[1];
                        if (left->column && isColumnConst(*left->column))
                        {
                            auto left_arg_type = left->result_type;
                            auto left_arg_value = (*left->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                func->function_base, left_arg_type, left_arg_value, const_type, const_value);
                        }
                        else
                        {
                            auto right_arg_type = right->result_type;
                            auto right_arg_value = (*right->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                func->function_base, const_type, const_value, right_arg_type, right_arg_value);
                        }
                    }
                }

                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                out_value = const_value;
                out_type = const_type;
                return true;
            }
        }
    }
    return false;
}


bool IcebergKeyCondition::isFunctionReallyMonotonic(const IFunctionBase & func, const IDataType & arg_type) const
{
    if (date_time_overflow_behavior_ignore && func.getName() == "toDateTime")
    {
        const IDataType * type = &arg_type;
        if (const auto * lowcard_type = typeid_cast<const DataTypeLowCardinality *>(type))
            type = lowcard_type->getDictionaryType().get();
        if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(type))
            type = nullable_type->getNestedType().get();

        /// toDateTime(date) may overflow, breaking monotonicity.
        if (isDateOrDate32(type))
            return false;
    }

    return true;
}

bool IcebergKeyCondition::canConstantBeWrappedByMonotonicFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node.getColumnName();

    if (array_joined_column_names.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
        return false;

    if (out_value.isNull())
        return false;

    MonotonicFunctionsChain transform_functions;
    auto can_transform_constant = extractMonotonicFunctionsChainFromKey(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_column_type,
        transform_functions,
        [this](const IFunctionBase & func, const IDataType & type)
        {
            if (!func.hasInformationAboutMonotonicity())
                return false;

            if (!isFunctionReallyMonotonic(func, type))
                return false;

            /// Range is irrelevant in this case.
            auto monotonicity = func.getMonotonicityForRange(type, Field(), Field());
            if (!monotonicity.is_always_monotonic)
                return false;

            return true;
        });

    if (!can_transform_constant)
        return false;

    auto const_column = out_type->createColumnConst(1, out_value);

    ColumnPtr transformed_const_column;
    DataTypePtr transformed_const_type;
    bool constant_transformed = DB::applyFunctionChainToColumn(
        const_column,
        out_type,
        transform_functions,
        transformed_const_column,
        transformed_const_type);

    if (!constant_transformed)
        return false;

    out_value = (*transformed_const_column)[0];
    out_type = transformed_const_type;
    return true;
}

/// Looking for possible transformation of `column = constant` into `partition_expr = function(constant)`
bool IcebergKeyCondition::canConstantBeWrappedByFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    Field & out_value,
    DataTypePtr & out_type)
{
    String expr_name = node.getColumnName();

    if (array_joined_column_names.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
    {
        /// Let's check another one case.
        /// If our storage was created with moduloLegacy in partition key,
        /// We can assume that `modulo(...) = const` is the same as `moduloLegacy(...) = const`.
        /// Replace modulo to moduloLegacy in AST and check if we also have such a column.
        ///
        /// We do not check this in canConstantBeWrappedByMonotonicFunctions.
        /// The case `f(modulo(...))` for totally monotonic `f ` is considered to be rare.
        ///
        /// Note: for negative values, we can filter more partitions then needed.
        expr_name = node.getColumnNameWithModuloLegacy();

        if (!key_subexpr_names.contains(expr_name))
            return false;
    }

    if (out_value.isNull())
        return false;

    MonotonicFunctionsChain transform_functions;
    auto can_transform_constant = extractMonotonicFunctionsChainFromKey(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_column_type,
        transform_functions,
        [](const IFunctionBase & func, const IDataType &) { return func.isDeterministic(); });

    if (!can_transform_constant)
        return false;

    auto const_column = out_type->createColumnConst(1, out_value);

    ColumnPtr transformed_const_column;
    DataTypePtr transformed_const_type;
    bool constant_transformed = DB::applyFunctionChainToColumn(
        const_column,
        out_type,
        transform_functions,
        transformed_const_column,
        transformed_const_type);

    if (!constant_transformed)
        return false;

    out_value = (*transformed_const_column)[0];
    out_type = transformed_const_type;
    return true;
}

bool IcebergKeyCondition::canConstantBeWrappedByMonotonicFunctions(
    const ASTPtr & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type)
{
    String expr_name = node->getColumnNameWithoutAlias();

    if (array_joined_column_names.count(expr_name))
        return false;

    if (key_subexpr_names.count(expr_name) == 0)
        return false;

    if (out_value.isNull())
        return false;

    return transformConstantWithValidFunctions(
        expr_name,
        out_key_column_num,
        out_key_column_type,
        out_value,
        out_type,
        [](const IFunctionBase & func, const IDataType & type)
        {
            if (!func.hasInformationAboutMonotonicity())
                return false;
            else
            {
                /// Range is irrelevant in this case.
                auto monotonicity = func.getMonotonicityForRange(type, Field(), Field());
                if (!monotonicity.is_always_monotonic)
                    return false;
            }
            return true;
        });
}

/// Looking for possible transformation of `column = constant` into `partition_expr = function(constant)`
bool IcebergKeyCondition::canConstantBeWrappedByFunctions(
    const ASTPtr & ast, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type)
{
    String expr_name = ast->getColumnNameWithoutAlias();

    if (array_joined_column_names.count(expr_name))
        return false;

    if (key_subexpr_names.count(expr_name) == 0)
    {
        /// Let's check another one case.
        /// If our storage was created with moduloLegacy in partition key,
        /// We can assume that `modulo(...) = const` is the same as `moduloLegacy(...) = const`.
        /// Replace modulo to moduloLegacy in AST and check if we also have such a column.
        ///
        /// We do not check this in canConstantBeWrappedByMonotonicFunctions.
        /// The case `f(modulo(...))` for totally monotonic `f ` is consedered to be rare.
        ///
        /// Note: for negative values, we can filter more partitions then needed.
        auto adjusted_ast = ast->clone();
        KeyDescription::moduloToModuloLegacyRecursive(adjusted_ast);
        expr_name = adjusted_ast->getColumnName();

        if (key_subexpr_names.count(expr_name) == 0)
            return false;
    }

    if (out_value.isNull())
        return false;

    return transformConstantWithValidFunctions(
        expr_name,
        out_key_column_num,
        out_key_column_type,
        out_value,
        out_type,
        [](const IFunctionBase & func, const IDataType &) { return func.isDeterministic(); });
}

static void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const String & node_column_name)
{
    try
    {
        src_value = convertFieldToType(src_value, *desired_type, src_type.get());
    }
    catch (...)
    {
        throw Exception(ErrorCodes::BAD_TYPE_OF_FIELD, "Key expression contains comparison between inconvertible types: "
            "{} and {} inside {}", desired_type->getName(), src_type->getName(), node_column_name);
    }
}

bool IcebergKeyCondition::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out)
{
    const auto * node_dag = node.getDAGNode();
    if (node_dag && node_dag->result_type->equals(DataTypeNullable(std::make_shared<DataTypeNothing>())))
    {
        /// If the inferred result type is Nullable(Nothing) at the query analysis stage,
        /// we don't analyze this node further as its condition will always be false.
        out.function = RPNElement::ALWAYS_FALSE;
        out.type = IcebergExpression::Type::FALSE;
        return true;
    }

    /** Functions < > = != <= >= in `notIn` isNull isNotNull, where one argument is a constant, and the other is one of columns of key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  (for example, if the table has ORDER BY time, we will check the conditions like
      *   toDate(time) = '2023-10-14', toMonth(time) = 12, etc)
      *  or any of arguments of a space-filling curve function if it is in the key,
      *  (for example, if the table has ORDER BY mortonEncode(x, y), we will check the conditions like x > c, y <= c, etc.)
      *  or constant expression - number
      *  (for example x AND 0)
      */
    Field const_value;
    DataTypePtr const_type;
    if (node.isFunction())
    {
        auto func = node.toFunctionNode();
        size_t num_args = func.getArgumentsSize();

        /// Type of expression containing key column
        DataTypePtr key_expr_type;

        /// Number of a key column (inside key_column_names array)
        size_t key_column_num = -1;

        /// For example, if the key is mortonEncode(x, y), and the atom is x, then the argument num is 0.
        std::optional<size_t> argument_num_of_space_filling_curve;

        MonotonicFunctionsChain chain;
        std::string func_name = func.getFunctionName();

        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        // auto analyze_point_in_polygon = [&, this]() -> bool
        // {
        //     /// pointInPolygon((x, y), [(0, 0), (8, 4), (5, 8), (0, 2)])
        //     if (func.getArgumentAt(0).tryGetConstant(const_value, const_type))
        //         return false;
        //     if (!func.getArgumentAt(1).tryGetConstant(const_value, const_type))
        //         return false;

        //     const auto atom_it = atom_map.find(func_name);

        //     /// Analyze (x, y)
        //     RPNElement::MultiColumnsFunctionDescription column_desc;
        //     column_desc.function_name = func_name;

        //     /// TODO: support index analysis for first argument of Point/Tuple type.
        //     if (!func.getArgumentAt(0).isFunction())
        //         return false;

        //     auto first_argument = func.getArgumentAt(0).toFunctionNode();
        //     if (first_argument.getArgumentsSize() != 2 || first_argument.getFunctionName() != "tuple")
        //         return false;

        //     for (size_t i = 0; i < 2; ++i)
        //     {
        //         auto name = first_argument.getArgumentAt(i).getColumnName();
        //         auto it = key_columns.find(name);
        //         if (it == key_columns.end())
        //             return false;
        //         column_desc.key_columns.push_back(name);
        //         column_desc.key_column_positions.push_back(key_columns[name]);
        //     }
        //     out.point_in_polygon_column_description = column_desc;

        //     /// Analyze [(0, 0), (8, 4), (5, 8), (0, 2)]
        //     chassert(WhichDataType(const_type).isArray());
        //     for (const auto & elem : const_value.safeGet<Array>())
        //     {
        //         if (elem.getType() != Field::Types::Tuple)
        //             return false;

        //         const auto & elem_tuple = elem.safeGet<Tuple>();
        //         if (elem_tuple.size() != 2)
        //             return false;

        //         auto x = applyVisitor(FieldVisitorConvertToNumber<Float64>(), elem_tuple[0]);
        //         auto y = applyVisitor(FieldVisitorConvertToNumber<Float64>(), elem_tuple[1]);
        //         out.polygon->data.outer().push_back({x, y});
        //     }
        //     boost::geometry::correct(out.polygon->data);
        //     return atom_it->second(out, const_value);
        // };

        bool allow_constant_transformation = !no_relaxed_atom_functions.contains(func_name);
        if (num_args == 1)
        {
            if (!(isKeyPossiblyWrappedByMonotonicFunctions(
                func.getArgumentAt(0), key_column_num, argument_num_of_space_filling_curve, key_expr_type, chain)))
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");
        }
        else if (num_args == 2)
        {
            size_t key_arg_pos;           /// Position of argument with key column (non-const argument)
            bool is_set_const = false;
            bool is_constant_transformed = false;

            if (functionIsInOrGlobalInOperator(func_name))
            {
                if (tryPrepareSetIndex(func, out, key_column_num, allow_constant_transformation, is_constant_transformed))
                {
                    key_arg_pos = 0;
                    is_set_const = true;
                }
                else
                    return false;
            }
            // else if (func_name == "pointInPolygon")
            // {
            //     /// Case1 no holes in polygon
            //     return analyze_point_in_polygon();
            // }
            else if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    out.type = IcebergExpression::Type::FALSE;
                    return true;
                }

                if (isKeyPossiblyWrappedByMonotonicFunctions(
                        func.getArgumentAt(0),
                        key_column_num,
                        argument_num_of_space_filling_curve,
                        key_expr_type,
                        chain,
                        single_point && func_name == "equals"))
                {
                    key_arg_pos = 0;
                }
                else if (
                    allow_constant_transformation
                    && canConstantBeWrappedByMonotonicFunctions(
                        func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals"
                    && canConstantBeWrappedByFunctions(func.getArgumentAt(0), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else if (func.getArgumentAt(0).tryGetConstant(const_value, const_type))
            {
                /// If the const operand is null, the atom will be always false
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    out.type = IcebergExpression::Type::FALSE;
                    return true;
                }

                if (isKeyPossiblyWrappedByMonotonicFunctions(
                        func.getArgumentAt(1),
                        key_column_num,
                        argument_num_of_space_filling_curve,
                        key_expr_type,
                        chain,
                        single_point && func_name == "equals"))
                {
                    key_arg_pos = 1;
                }
                else if (
                    allow_constant_transformation
                    && canConstantBeWrappedByMonotonicFunctions(
                        func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 1;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals"
                    && canConstantBeWrappedByFunctions(func.getArgumentAt(1), key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");

            /// Replace <const> <sign> <data> on to <data> <-sign> <const>
            if (key_arg_pos == 1)
            {
                if (func_name == "less")
                    func_name = "greater";
                else if (func_name == "greater")
                    func_name = "less";
                else if (func_name == "greaterOrEquals")
                    func_name = "lessOrEquals";
                else if (func_name == "lessOrEquals")
                    func_name = "greaterOrEquals";
                else if (func_name == "in" || func_name == "notIn" ||
                         func_name == "like" || func_name == "notLike" ||
                         func_name == "ilike" || func_name == "notILike" ||
                         func_name == "startsWith" || func_name == "match")
                {
                    /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                    return false;
                }
            }

            key_expr_type = recursiveRemoveLowCardinality(key_expr_type);
            DataTypePtr key_expr_type_not_null;
            bool key_expr_type_is_nullable = false;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(key_expr_type.get()))
            {
                key_expr_type_is_nullable = true;
                key_expr_type_not_null = nullable_type->getNestedType();
            }
            else
                key_expr_type_not_null = key_expr_type;

            bool cast_not_needed = is_set_const /// Set args are already cast inside Set::createFromAST
                || ((isNativeInteger(key_expr_type_not_null) || isDateTime(key_expr_type_not_null))
                    && (isNativeInteger(const_type) || isDateTime(const_type))); /// Native integers and DateTime are accurately compared without cast.

            if (!cast_not_needed && !key_expr_type_not_null->equals(*const_type))
            {
                if (const_value.getType() == Field::Types::String)
                {
                    const_value = convertFieldToType(const_value, *key_expr_type_not_null);
                    if (const_value.isNull())
                        return false;
                    // No need to set is_constant_transformed because we're doing exact conversion
                }
                else
                {
                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{key_expr_type_not_null, const_type});
                    if (!common_type)
                        return false;

                    if (!const_type->equals(*common_type))
                    {
                        castValueToType(common_type, const_value, const_type, node.getColumnName());

                        // Need to set is_constant_transformed unless we're doing exact conversion
                        if (!key_expr_type_not_null->equals(*common_type))
                            is_constant_transformed = true;
                    }
                    if (!key_expr_type_not_null->equals(*common_type))
                    {
                        auto common_type_maybe_nullable = (key_expr_type_is_nullable && !common_type->isNullable())
                            ? DataTypePtr(std::make_shared<DataTypeNullable>(common_type))
                            : common_type;

                        auto func_cast = createInternalCast({key_expr_type, {}}, common_type_maybe_nullable, CastType::nonAccurate, {});

                        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
                        if (!single_point && !func_cast->hasInformationAboutMonotonicity())
                            return false;
                        chain.push_back(func_cast);
                    }
                }
            }

            /// Transformed constant must weaken the condition, for example "x > 5" must weaken to "round(x) >= 5"
            if (is_constant_transformed)
            {
                if (func_name == "less")
                    func_name = "lessOrEquals";
                else if (func_name == "greater")
                    func_name = "greaterOrEquals";

                relaxed = true;
            }

        }
        // else
        // {
        //     if (func_name == "pointInPolygon")
        //     {
        //         /// Case2 has holes in polygon, when checking skip index, the hole will be ignored.
        //         return analyze_point_in_polygon();
        //     }

        //     return false;
        // }

        const auto atom_it = atom_map.find(func_name);

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);
        out.argument_num_of_space_filling_curve = argument_num_of_space_filling_curve;

        bool valid_atom = atom_it->second(out, const_value);
        if (valid_atom && out.relaxed)
            relaxed = true;
        return valid_atom;
    }
    if (node.tryGetConstant(const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`

        if (const_value.getType() == Field::Types::UInt64)
        {
            out.function = const_value.safeGet<UInt64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            out.type = const_value.safeGet<UInt64>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
        if (const_value.getType() == Field::Types::Int64)
        {
            out.function = const_value.safeGet<Int64>() ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            out.type = const_value.safeGet<Int64>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
        if (const_value.getType() == Field::Types::Float64)
        {
            out.function = const_value.safeGet<Float64>() != 0.0 ? RPNElement::ALWAYS_TRUE : RPNElement::ALWAYS_FALSE;
            out.type = const_value.safeGet<Float64>() != 0.0 ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
    }
    return false;
}

bool IcebergKeyCondition::tryPrepareSetIndex(
    const RPNBuilderFunctionTreeNode & func,
    RPNElement & out,
    size_t & out_key_column_num,
    bool & allow_constant_transformation,
    bool & is_constant_transformed)
{
    const auto & left_arg = func.getArgumentAt(0);

    out_key_column_num = 0;
    std::vector<MergeTreeSetIndex::KeyTuplePositionMapping> indexes_mapping;
    std::vector<MonotonicFunctionsChain> set_transforming_chains;
    DataTypes data_types;

    auto get_key_tuple_position_mapping = [&](const RPNBuilderTreeNode & node, size_t tuple_index)
    {
        MergeTreeSetIndex::KeyTuplePositionMapping index_mapping;
        index_mapping.tuple_index = tuple_index;
        DataTypePtr data_type;
        std::optional<size_t> key_space_filling_curve_argument_pos;
        MonotonicFunctionsChain set_transforming_chain;
        if (isKeyPossiblyWrappedByMonotonicFunctions(
                node, index_mapping.key_index, key_space_filling_curve_argument_pos, data_type, index_mapping.functions)
            && !key_space_filling_curve_argument_pos) /// We don't support the analysis of space-filling curves and IN set.
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            out_key_column_num = std::max(out_key_column_num, index_mapping.key_index);
            set_transforming_chains.push_back(set_transforming_chain);
        }
        // For partition index, checking if set can be transformed to prune any partitions
        else if (
            single_point && allow_constant_transformation
            && canSetValuesBeWrappedByFunctions(node, index_mapping.key_index, data_type, set_transforming_chain))
        {
            indexes_mapping.push_back(index_mapping);
            data_types.push_back(data_type);
            out_key_column_num = std::max(out_key_column_num, index_mapping.key_index);
            set_transforming_chains.push_back(set_transforming_chain);
        }
    };

    size_t left_args_count = 1;
    if (left_arg.isFunction())
    {
        /// Note: in case of ActionsDAG, tuple may be a constant.
        /// In this case, there is no keys in tuple. So, we don't have to check it.
        auto left_arg_tuple = left_arg.toFunctionNode();
        if (left_arg_tuple.getFunctionName() == "tuple" && left_arg_tuple.getArgumentsSize() > 1)
        {
            left_args_count = left_arg_tuple.getArgumentsSize();
            for (size_t i = 0; i < left_args_count; ++i)
                get_key_tuple_position_mapping(left_arg_tuple.getArgumentAt(i), i);
        }
        else
        {
            get_key_tuple_position_mapping(left_arg, 0);
        }
    }
    else
    {
        get_key_tuple_position_mapping(left_arg, 0);
    }

    if (indexes_mapping.empty())
        return false;

    const auto right_arg = func.getArgumentAt(1);

    auto future_set = right_arg.tryGetPreparedSet();
    if (!future_set)
        return false;

    auto prepared_set = future_set->buildOrderedSetInplace(right_arg.getTreeContext().getQueryContext());
    if (!prepared_set)
        return false;

    /// The index can be prepared if the elements of the set were saved in advance.
    if (!prepared_set->hasExplicitSetElements())
        return false;

    /** Try to convert set columns to primary key columns.
      * Example: SELECT id FROM test_table WHERE id IN (SELECT 1);
      * In this example table `id` column has type UInt64, Set column has type UInt8. To use index
      * we need to convert set column to primary key column.
      */
    auto set_columns = prepared_set->getSetElements();
    auto set_types = future_set->getTypes();
    {
        Columns new_columns;
        DataTypes new_types;
        while (set_columns.size() < left_args_count) /// If we have an unpacked tuple inside, we unpack it
        {
            bool has_tuple = false;
            for (size_t i = 0; i < set_columns.size(); ++i)
            {
                if (isTuple(set_types[i]))
                {
                    has_tuple = true;
                    auto columns_tuple = assert_cast<const ColumnTuple*>(set_columns[i].get())->getColumns();
                    auto subtypes = assert_cast<const DataTypeTuple&>(*set_types[i]).getElements();
                    new_columns.insert(new_columns.end(), columns_tuple.begin(), columns_tuple.end());
                    new_types.insert(new_types.end(), subtypes.begin(), subtypes.end());
                }
                else
                {
                    new_columns.push_back(set_columns[i]);
                    new_types.push_back(set_types[i]);
                }
            }
            if (!has_tuple)
                return false;

            set_columns.swap(new_columns);
            set_types.swap(new_types);
            new_columns.clear();
            new_types.clear();
        }
    }
    size_t set_types_size = set_types.size();
    size_t indexes_mapping_size = indexes_mapping.size();
    assert(set_types_size == set_columns.size());

    Columns transformed_set_columns = set_columns;

    for (size_t indexes_mapping_index = 0; indexes_mapping_index < indexes_mapping_size; ++indexes_mapping_index)
    {
        const auto & key_column_type = data_types[indexes_mapping_index];
        size_t set_element_index = indexes_mapping[indexes_mapping_index].tuple_index;
        auto set_element_type = set_types[set_element_index];
        ColumnPtr set_column = set_columns[set_element_index];

        if (!set_transforming_chains[indexes_mapping_index].empty())
        {
            ColumnPtr transformed_set_column;
            DataTypePtr transformed_set_type;
            if (!DB::applyFunctionChainToColumn(
                    set_column,
                    set_element_type,
                    set_transforming_chains[indexes_mapping_index],
                    transformed_set_column,
                    transformed_set_type))
                return false;

            set_column = transformed_set_column;
            set_element_type = transformed_set_type;
            is_constant_transformed = true;
        }

        if (canBeSafelyCast(set_element_type, key_column_type))
        {
            transformed_set_columns[set_element_index] = castColumn({set_column, set_element_type, {}}, key_column_type);
            continue;
        }

        if (!key_column_type->canBeInsideNullable())
            return false;

        const NullMap * set_column_null_map = nullptr;

        // Keep a reference to the original set_column to ensure the data remains valid
        ColumnPtr original_set_column = set_column;

        if (isNullableOrLowCardinalityNullable(set_element_type))
        {
            if (WhichDataType(set_element_type).isLowCardinality())
            {
                set_element_type = removeLowCardinality(set_element_type);
                transformed_set_columns[set_element_index] = set_column->convertToFullColumnIfLowCardinality();
            }

            set_element_type = removeNullable(set_element_type);

            // Obtain the nullable column without reassigning set_column immediately
            const auto * set_column_nullable = typeid_cast<const ColumnNullable *>(transformed_set_columns[set_element_index].get());
            if (!set_column_nullable)
                return false;

            const NullMap & null_map_data = set_column_nullable->getNullMapData();
            if (!null_map_data.empty())
                set_column_null_map = &null_map_data;

            ColumnPtr nested_column = set_column_nullable->getNestedColumnPtr();

            // Reassign set_column after we have obtained necessary references
            set_column = nested_column;
        }

        ColumnPtr nullable_set_column = castColumnAccurateOrNull({set_column, set_element_type, {}}, key_column_type);
        const auto * nullable_set_column_typed = typeid_cast<const ColumnNullable *>(nullable_set_column.get());
        if (!nullable_set_column_typed)
            return false;

        const NullMap & nullable_set_column_null_map = nullable_set_column_typed->getNullMapData();
        size_t nullable_set_column_null_map_size = nullable_set_column_null_map.size();

        IColumn::Filter filter(nullable_set_column_null_map_size);

        if (set_column_null_map)
        {
            for (size_t i = 0; i < nullable_set_column_null_map_size; ++i)
            {
                if (nullable_set_column_null_map_size < set_column_null_map->size())
                    filter[i] = (*set_column_null_map)[i] || !nullable_set_column_null_map[i];
                else
                    filter[i] = !nullable_set_column_null_map[i];
            }

            set_column = nullable_set_column_typed->filter(filter, 0);
        }
        else
        {
            for (size_t i = 0; i < nullable_set_column_null_map_size; ++i)
                filter[i] = !nullable_set_column_null_map[i];

            set_column = nullable_set_column_typed->getNestedColumn().filter(filter, 0);
        }

        transformed_set_columns[set_element_index] = std::move(set_column);
    }

    set_columns = std::move(transformed_set_columns);

    out.set_index = std::make_shared<MergeTreeSetIndex>(set_columns, std::move(indexes_mapping));

    /// When not all key columns are used or when there are multiple elements in
    /// the set, the atom's hyperrectangle is expanded to encompass the missing
    /// dimensions and any "gaps".
    if (indexes_mapping_size < set_types_size || out.set_index->size() > 1)
        relaxed = true;

    return true;
}

/** Allow to use two argument function with constant argument to be analyzed as a single argument function.
  * In other words, it performs "currying" (binding of arguments).
  * This is needed, for example, to support correct analysis of `toDate(time, 'UTC')`.
  */
class FunctionWithOptionalConstArg : public IFunctionBase
{
public:
    enum Kind
    {
        NO_CONST = 0,
        LEFT_CONST,
        RIGHT_CONST,
    };

    explicit FunctionWithOptionalConstArg(const FunctionBasePtr & func_) : func(func_) { }
    FunctionWithOptionalConstArg(const FunctionBasePtr & func_, const ColumnWithTypeAndName & const_arg_, Kind kind_)
        : func(func_), const_arg(const_arg_), kind(kind_)
    {
    }

    String getName() const override { return func->getName(); }

    const DataTypes & getArgumentTypes() const override { return func->getArgumentTypes(); }

    const DataTypePtr & getResultType() const override { return func->getResultType(); }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName & arguments) const override { return func->prepare(arguments); }

    ColumnPtr
    execute(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count, bool dry_run) const override
    {
        if (kind == Kind::LEFT_CONST)
        {
            ColumnsWithTypeAndName new_arguments;
            new_arguments.reserve(arguments.size() + 1);
            new_arguments.push_back(const_arg);
            for (const auto & arg : arguments)
                new_arguments.push_back(arg);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        else if (kind == Kind::RIGHT_CONST)
        {
            auto new_arguments = arguments;
            new_arguments.push_back(const_arg);
            return func->prepare(new_arguments)->execute(new_arguments, result_type, input_rows_count, dry_run);
        }
        else
            return func->prepare(arguments)->execute(arguments, result_type, input_rows_count, dry_run);
    }

    bool isDeterministic() const override { return func->isDeterministic(); }

    bool isDeterministicInScopeOfQuery() const override { return func->isDeterministicInScopeOfQuery(); }

    bool hasInformationAboutMonotonicity() const override { return func->hasInformationAboutMonotonicity(); }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & arguments) const override
    {
        return func->isSuitableForShortCircuitArgumentsExecution(arguments);
    }

    IFunctionBase::Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const override
    {
        return func->getMonotonicityForRange(type, left, right);
    }

    Kind getKind() const { return kind; }
    const ColumnWithTypeAndName & getConstArg() const { return const_arg; }

private:
    FunctionBasePtr func;
    ColumnWithTypeAndName const_arg;
    Kind kind = Kind::NO_CONST;
};


bool IcebergKeyCondition::isKeyPossiblyWrappedByMonotonicFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    std::optional<size_t> & out_argument_num_of_space_filling_curve,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain,
    bool assume_function_monotonicity)
{
    std::vector<RPNBuilderFunctionTreeNode> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        node, out_key_column_num, out_argument_num_of_space_filling_curve, key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        auto function = *it;
        auto func_builder = FunctionFactory::instance().tryGet(function.getFunctionName(), node.getTreeContext().getQueryContext());
        if (!func_builder)
            return false;
        ColumnsWithTypeAndName arguments;
        ColumnWithTypeAndName const_arg;
        FunctionWithOptionalConstArg::Kind kind = FunctionWithOptionalConstArg::Kind::NO_CONST;
        if (function.getArgumentsSize() == 2)
        {
            if (function.getArgumentAt(0).isConstant())
            {
                const_arg = function.getArgumentAt(0).getConstantColumn();
                arguments.push_back(const_arg);
                arguments.push_back({ nullptr, key_column_type, "" });
                kind = FunctionWithOptionalConstArg::Kind::LEFT_CONST;
            }
            else if (function.getArgumentAt(1).isConstant())
            {
                arguments.push_back({ nullptr, key_column_type, "" });
                const_arg = function.getArgumentAt(1).getConstantColumn();
                arguments.push_back(const_arg);
                kind = FunctionWithOptionalConstArg::Kind::RIGHT_CONST;
            }

            /// If constant arg of binary operator is NULL, there will be no monotonicity.
            if (const_arg.column->isNullAt(0))
                return false;
        }
        else
            arguments.push_back({ nullptr, key_column_type, "" });
        auto func = func_builder->build(arguments);

        if (!func || !func->isDeterministicInScopeOfQuery() || (!assume_function_monotonicity && !func->hasInformationAboutMonotonicity()))
            return false;

        if (!isFunctionReallyMonotonic(*func, *key_column_type))
            return false;

        key_column_type = func->getResultType();
        if (kind == FunctionWithOptionalConstArg::Kind::NO_CONST)
            out_functions_chain.push_back(func);
        else
            out_functions_chain.push_back(std::make_shared<FunctionWithOptionalConstArg>(func, const_arg, kind));
    }

    out_key_res_column_type = key_column_type;

    return true;
}

bool IcebergKeyCondition::isKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    std::optional<size_t> & out_argument_num_of_space_filling_curve,
    DataTypePtr & out_key_column_type,
    std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = key_expr->getSampleBlock();

    /// Key columns should use canonical names for the index analysis.
    String name = node.getColumnName();

    if (array_joined_column_names.contains(name))
        return false;

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(name).type;
        return true;
    }

    /** The case of space-filling curves.
      * When the node is not a key column (e.g. mortonEncode(x, y))
      * but one of the arguments of a key column (e.g. x or y).
      *
      * For example, the table has ORDER BY mortonEncode(x, y)
      * and query has WHERE x >= 10 AND x < 15 AND y > 20 AND y <= 25
      */
    // for (const auto & curve : key_space_filling_curves)
    // {
    //     for (size_t i = 0, size = curve.arguments.size(); i < size; ++i)
    //     {
    //         if (curve.arguments[i] == name)
    //         {
    //             out_key_column_num = curve.key_column_pos;
    //             out_argument_num_of_space_filling_curve = i;
    //             out_key_column_type = sample_block.getByName(name).type;
    //             return true;
    //         }
    //     }
    // }

    if (node.isFunction())
    {
        auto function_node = node.toFunctionNode();

        size_t arguments_size = function_node.getArgumentsSize();
        if (arguments_size > 2 || arguments_size == 0)
            return false;

        out_functions_chain.push_back(function_node);

        bool result = false;
        if (arguments_size == 2)
        {
            if (function_node.getArgumentAt(0).isConstant())
            {
                result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(
                    function_node.getArgumentAt(1),
                    out_key_column_num,
                    out_argument_num_of_space_filling_curve,
                    out_key_column_type,
                    out_functions_chain);
            }
            else if (function_node.getArgumentAt(1).isConstant())
            {
                result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(
                    function_node.getArgumentAt(0),
                    out_key_column_num,
                    out_argument_num_of_space_filling_curve,
                    out_key_column_type,
                    out_functions_chain);
            }
        }
        else
        {
            result = isKeyPossiblyWrappedByMonotonicFunctionsImpl(
                function_node.getArgumentAt(0),
                out_key_column_num,
                out_argument_num_of_space_filling_curve,
                out_key_column_type,
                out_functions_chain);
        }

        return result;
    }

    return false;
}

/** When table's key has expression with these functions from a column,
  * and when a column in a query is compared with a constant, such as:
  * CREATE TABLE (x String) ORDER BY toDate(x)
  * SELECT ... WHERE x LIKE 'Hello%'
  * we want to apply the function to the constant for index analysis,
  * but should modify it to pass on un-parsable values.
  */
static std::set<std::string_view> date_time_parsing_functions = {
    "toDate",
    "toDate32",
    "toDateTime",
    "toDateTime64",
    "parseDateTimeBestEffort",
    "parseDateTimeBestEffortUS",
    "parseDateTime32BestEffort",
    "parseDateTime64BestEffort",
    "parseDateTime",
    "parseDateTimeInJodaSyntax",
};

/** The key functional expression constraint may be inferred from a plain column in the expression.
  * For example, if the key contains `toStartOfHour(Timestamp)` and query contains `WHERE Timestamp >= now()`,
  * it can be assumed that if `toStartOfHour()` is monotonic on [now(), inf), the `toStartOfHour(Timestamp) >= toStartOfHour(now())`
  * condition also holds, so the index may be used to select only parts satisfying this condition.
  *
  * To check the assumption, we'd need to assert that the inverse function to this transformation is also monotonic, however the
  * inversion isn't exported (or even viable for not strictly monotonic functions such as `toStartOfHour()`).
  * Instead, we can qualify only functions that do not transform the range (for example rounding),
  * which while not strictly monotonic, are monotonic everywhere on the input range.
  */
bool IcebergKeyCondition::extractMonotonicFunctionsChainFromKey(
    ContextPtr context,
    const String & expr_name,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    MonotonicFunctionsChain & out_functions_chain,
    std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const
{
    const auto & sample_block = key_expr->getSampleBlock();

    for (const auto & node : key_expr->getNodes())
    {
        auto it = key_columns.find(node.result_name);
        if (it != key_columns.end())
        {
            std::stack<const ActionsDAG::Node *> chain;

            const auto * cur_node = &node;
            bool is_valid_chain = true;

            while (is_valid_chain)
            {
                if (cur_node->result_name == expr_name)
                    break;

                chain.push(cur_node);

                if (cur_node->type == ActionsDAG::ActionType::FUNCTION && cur_node->children.size() <= 2)
                {
                    is_valid_chain = always_monotonic(*cur_node->function_base, *cur_node->result_type);

                    const ActionsDAG::Node * next_node = nullptr;
                    for (const auto * arg : cur_node->children)
                    {
                        if (arg->column && isColumnConst(*arg->column))
                            continue;

                        if (next_node)
                            is_valid_chain = false;

                        next_node = arg;
                    }

                    if (!next_node)
                        is_valid_chain = false;

                    cur_node = next_node;
                }
                else if (cur_node->type == ActionsDAG::ActionType::ALIAS)
                    cur_node = cur_node->children.front();
                else
                    is_valid_chain = false;
            }

            if (is_valid_chain)
            {
                while (!chain.empty())
                {
                    const auto * func = chain.top();
                    chain.pop();

                    if (func->type != ActionsDAG::ActionType::FUNCTION)
                        continue;

                    auto func_name = func->function_base->getName();
                    auto func_base = func->function_base;

                    ColumnsWithTypeAndName arguments;
                    ColumnWithTypeAndName const_arg;
                    FunctionWithOptionalConstArg::Kind kind = FunctionWithOptionalConstArg::Kind::NO_CONST;

                    if (date_time_parsing_functions.contains(func_name))
                    {
                        const auto & arg_types = func_base->getArgumentTypes();
                        if (!arg_types.empty() && isStringOrFixedString(arg_types[0]))
                            func_name = func_name + "OrNull";
                    }

                    auto func_builder = FunctionFactory::instance().tryGet(func_name, context);

                    if (func->children.size() == 1)
                    {
                        arguments.push_back({nullptr, removeLowCardinality(func->children[0]->result_type), ""});
                    }
                    else if (func->children.size() == 2)
                    {
                        const auto * left = func->children[0];
                        const auto * right = func->children[1];
                        if (left->column && isColumnConst(*left->column))
                        {
                            const_arg = {left->result_type->createColumnConst(0, (*left->column)[0]), left->result_type, ""};
                            arguments.push_back(const_arg);
                            arguments.push_back({nullptr, removeLowCardinality(right->result_type), ""});
                            kind = FunctionWithOptionalConstArg::Kind::LEFT_CONST;
                        }
                        else
                        {
                            const_arg = {right->result_type->createColumnConst(0, (*right->column)[0]), right->result_type, ""};
                            arguments.push_back({nullptr, removeLowCardinality(left->result_type), ""});
                            arguments.push_back(const_arg);
                            kind = FunctionWithOptionalConstArg::Kind::RIGHT_CONST;
                        }
                    }

                    auto out_func = func_builder->build(arguments);
                    if (kind == FunctionWithOptionalConstArg::Kind::NO_CONST)
                        out_functions_chain.push_back(out_func);
                    else
                        out_functions_chain.push_back(std::make_shared<FunctionWithOptionalConstArg>(out_func, const_arg, kind));
                }

                out_key_column_num = it->second;
                out_key_column_type = sample_block.getByName(it->first).type;
                return true;
            }
        }
    }

    return false;
}

bool IcebergKeyCondition::canSetValuesBeWrappedByFunctions(
    const RPNBuilderTreeNode & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain)
{
    // Checking if column name matches any of key subexpressions
    String expr_name = node.getColumnName();

    if (array_joined_column_names.contains(expr_name))
        return false;

    if (!key_subexpr_names.contains(expr_name))
    {
        expr_name = node.getColumnNameWithModuloLegacy();

        if (!key_subexpr_names.contains(expr_name))
            return false;
    }

    return extractMonotonicFunctionsChainFromKey(
        node.getTreeContext().getQueryContext(),
        expr_name,
        out_key_column_num,
        out_key_res_column_type,
        out_functions_chain,
        [](const IFunctionBase & func, const IDataType &)
        {
            return func.isDeterministic();
        });
}


bool IcebergKeyCondition::isKeyPossiblyWrappedByMonotonicFunctions(
    const ASTPtr & node,
    ContextPtr context,
    size_t & out_key_column_num,
    DataTypePtr & out_key_res_column_type,
    MonotonicFunctionsChain & out_functions_chain)
{
    std::vector<const ASTFunction *> chain_not_tested_for_monotonicity;
    DataTypePtr key_column_type;

    if (!isKeyPossiblyWrappedByMonotonicFunctionsImpl(node, out_key_column_num, key_column_type, chain_not_tested_for_monotonicity))
        return false;

    for (auto it = chain_not_tested_for_monotonicity.rbegin(); it != chain_not_tested_for_monotonicity.rend(); ++it)
    {
        const auto & args = (*it)->arguments->children;
        auto func_builder = FunctionFactory::instance().tryGet((*it)->name, context);
        if (!func_builder)
            return false;
        ColumnsWithTypeAndName arguments;
        ColumnWithTypeAndName const_arg;
        FunctionWithOptionalConstArg::Kind kind = FunctionWithOptionalConstArg::Kind::NO_CONST;
        if (args.size() == 2)
        {
            if (const auto * arg_left = args[0]->as<ASTLiteral>())
            {
                auto left_arg_type = applyVisitor(FieldToDataType(), arg_left->value);
                const_arg = {left_arg_type->createColumnConst(0, arg_left->value), left_arg_type, ""};
                arguments.push_back(const_arg);
                arguments.push_back({nullptr, key_column_type, ""});
                kind = FunctionWithOptionalConstArg::Kind::LEFT_CONST;
            }
            else if (const auto * arg_right = args[1]->as<ASTLiteral>())
            {
                arguments.push_back({nullptr, key_column_type, ""});
                auto right_arg_type = applyVisitor(FieldToDataType(), arg_right->value);
                const_arg = {right_arg_type->createColumnConst(0, arg_right->value), right_arg_type, ""};
                arguments.push_back(const_arg);
                kind = FunctionWithOptionalConstArg::Kind::RIGHT_CONST;
            }
        }
        else
            arguments.push_back({nullptr, key_column_type, ""});
        auto func = func_builder->build(arguments);

        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
        if (!func || (!single_point && !func->hasInformationAboutMonotonicity()))
            return false;

        key_column_type = func->getResultType();
        if (kind == FunctionWithOptionalConstArg::Kind::NO_CONST)
            out_functions_chain.push_back(func);
        else
            out_functions_chain.push_back(std::make_shared<FunctionWithOptionalConstArg>(func, const_arg, kind));
    }

    out_key_res_column_type = key_column_type;

    return true;
}

bool IcebergKeyCondition::isKeyPossiblyWrappedByMonotonicFunctionsImpl(
    const ASTPtr & node,
    size_t & out_key_column_num,
    DataTypePtr & out_key_column_type,
    std::vector<const ASTFunction *> & out_functions_chain)
{
    /** By itself, the key column can be a functional expression. for example, `intHash32(UserID)`.
      * Therefore, use the full name of the expression for search.
      */
    const auto & sample_block = key_expr->getSampleBlock();

    // Key columns should use canonical names for index analysis
    String name = node->getColumnNameWithoutAlias();

    if (array_joined_column_names.count(name))
        return false;

    auto it = key_columns.find(name);
    if (key_columns.end() != it)
    {
        out_key_column_num = it->second;
        out_key_column_type = sample_block.getByName(it->first).type;
        return true;
    }

    if (const auto * func = node->as<ASTFunction>())
    {
        if (!func->arguments)
            return false;

        const auto & args = func->arguments->children;
        if (args.size() > 2 || args.empty())
            return false;

        out_functions_chain.push_back(func);
        bool ret = false;
        if (args.size() == 2)
        {
            if (args[0]->as<ASTLiteral>())
            {
                ret = isKeyPossiblyWrappedByMonotonicFunctionsImpl(args[1], out_key_column_num, out_key_column_type, out_functions_chain);
            }
            else if (args[1]->as<ASTLiteral>())
            {
                ret = isKeyPossiblyWrappedByMonotonicFunctionsImpl(args[0], out_key_column_num, out_key_column_type, out_functions_chain);
            }
        }
        else
        {
            ret = isKeyPossiblyWrappedByMonotonicFunctionsImpl(args[0], out_key_column_num, out_key_column_type, out_functions_chain);
        }
        return ret;
    }

    return false;
}


static void castValueToType(const DataTypePtr & desired_type, Field & src_value, const DataTypePtr & src_type, const ASTPtr & node)
{
    try
    {
        src_value = convertFieldToType(src_value, *desired_type, src_type.get());
    }
    catch (...)
    {
        WriteBufferFromOwnString buf;
        IAST::FormatSettings settings(true);
        settings.hilite = true;
        IAST::FormatState state;
        IAST::FormatStateStacked state_stacked;
        IAST::FormattingBuffer format_buf{buf, settings, state, state_stacked};
        node->format(format_buf);

        throw Exception(
            ErrorCodes::BAD_TYPE_OF_FIELD,
            "Key expression contains comparison between inconvertible types: {} and {} inside {}", 
            desired_type->getName(), 
            src_type->getName(), 
            buf.str());
    }
}


bool IcebergKeyCondition::tryParseAtomFromAST(const ASTPtr & node, ContextPtr context, Block & block_with_constants, RPNElement & out)
{
    /** Functions < > = != <= >= in `notIn` isNull isNotNull, where one argument is a constant, and the other is one of columns of key,
      *  or itself, wrapped in a chain of possibly-monotonic functions,
      *  or constant expression - number.
      */
    Field const_value;
    DataTypePtr const_type;
    if (const auto * func = node->as<ASTFunction>())
    {
        const ASTs & args = func->arguments->children;

        DataTypePtr key_expr_type; /// Type of expression containing key column
        size_t key_column_num = -1; /// Number of a key column (inside key_column_names array)
        MonotonicFunctionsChain chain;
        std::string func_name = func->name;

        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        if (args.size() == 1)
        {
            if (!(isKeyPossiblyWrappedByMonotonicFunctions(args[0], context, key_column_num, key_expr_type, chain)))
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");
        }
        else if (args.size() == 2)
        {
            size_t key_arg_pos; /// Position of argument with key column (non-const argument)
            bool is_set_const = false;
            bool is_constant_transformed = false;

            /// We don't look for inversed key transformations when strict is true, which is required for trivial count().
            /// Consider the following test case:
            ///
            /// create table test1(p DateTime, k int) engine MergeTree partition by toDate(p) order by k;
            /// insert into test1 values ('2020-09-01 00:01:02', 1), ('2020-09-01 20:01:03', 2), ('2020-09-02 00:01:03', 3);
            /// select count() from test1 where p > toDateTime('2020-09-01 10:00:00');
            ///
            /// toDate(DateTime) is always monotonic, but we cannot relax the predicates to be
            /// >= toDate(toDateTime('2020-09-01 10:00:00')), which returns 3 instead of the right count: 2.
            bool strict_condition = strict;

            /// If we use this key condition to prune partitions by single value, we cannot relax conditions for NOT.
            if (single_point
                && (func_name == "notLike" || func_name == "notIn" || func_name == "globalNotIn" || func_name == "notNullIn"
                    || func_name == "globalNotNullIn" || func_name == "notEquals" || func_name == "notEmpty"))
                strict_condition = true;

            if (getConstant(args[1], block_with_constants, const_value, const_type))
            {
                if (isKeyPossiblyWrappedByMonotonicFunctions(args[0], context, key_column_num, key_expr_type, chain))
                {
                    key_arg_pos = 0;
                }
                else if (
                    !strict_condition
                    && canConstantBeWrappedByMonotonicFunctions(args[0], key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals" && !strict_condition
                    && canConstantBeWrappedByFunctions(args[0], key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else if (getConstant(args[0], block_with_constants, const_value, const_type))
            {
                if (isKeyPossiblyWrappedByMonotonicFunctions(args[1], context, key_column_num, key_expr_type, chain))
                {
                    key_arg_pos = 1;
                }
                else if (
                    !strict_condition
                    && canConstantBeWrappedByMonotonicFunctions(args[1], key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 1;
                    is_constant_transformed = true;
                }
                else if (
                    single_point && func_name == "equals" && !strict_condition
                    && canConstantBeWrappedByFunctions(args[1], key_column_num, key_expr_type, const_value, const_type))
                {
                    key_arg_pos = 0;
                    is_constant_transformed = true;
                }
                else
                    return false;
            }
            else
                return false;

            if (key_column_num == static_cast<size_t>(-1))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "`key_column_num` wasn't initialized. It is a bug.");

            /// Replace <const> <sign> <data> on to <data> <-sign> <const>
            if (key_arg_pos == 1)
            {
                if (func_name == "less")
                    func_name = "greater";
                else if (func_name == "greater")
                    func_name = "less";
                else if (func_name == "greaterOrEquals")
                    func_name = "lessOrEquals";
                else if (func_name == "lessOrEquals")
                    func_name = "greaterOrEquals";
                else if (
                    func_name == "in" || func_name == "notIn" || func_name == "like" || func_name == "notLike" || func_name == "ilike"
                    || func_name == "notIlike" || func_name == "startsWith")
                {
                    /// "const IN data_column" doesn't make sense (unlike "data_column IN const")
                    return false;
                }
            }

            key_expr_type = recursiveRemoveLowCardinality(key_expr_type);
            DataTypePtr key_expr_type_not_null;
            bool key_expr_type_is_nullable = false;
            if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(key_expr_type.get()))
            {
                key_expr_type_is_nullable = true;
                key_expr_type_not_null = nullable_type->getNestedType();
            }
            else
                key_expr_type_not_null = key_expr_type;

            if (const_value.getType() == Field::Types::Array || const_value.getType() == Field::Types::Tuple)
                is_set_const = true;

            bool cast_not_needed = is_set_const /// Set args are already casted inside Set::createFromAST
                || ((isNativeInteger(key_expr_type_not_null) || isDateTime(key_expr_type_not_null))
                    && (isNativeInteger(const_type)
                        || isDateTime(const_type))); /// Native integers and DateTime are accurately compared without cast.

            if (!cast_not_needed && !key_expr_type_not_null->equals(*const_type))
            {
                if (const_value.getType() == Field::Types::String)
                {
                    const_value = convertFieldToType(const_value, *key_expr_type_not_null);
                    if (const_value.isNull())
                        return false;
                    // No need to set is_constant_transformed because we're doing exact conversion
                }
                else
                {
                    DataTypePtr common_type = tryGetLeastSupertype(DataTypes{key_expr_type_not_null, const_type});
                    if (!common_type)
                        return false;

                    if (!const_type->equals(*common_type))
                    {
                        castValueToType(common_type, const_value, const_type, node);

                        // Need to set is_constant_transformed unless we're doing exact conversion
                        if (!key_expr_type_not_null->equals(*common_type))
                            is_constant_transformed = true;
                    }
                    if (!key_expr_type_not_null->equals(*common_type))
                    {
                        auto common_type_maybe_nullable = (key_expr_type_is_nullable && !common_type->isNullable())
                            ? DataTypePtr(std::make_shared<DataTypeNullable>(common_type))
                            : common_type;

                        auto func_cast = createInternalCast({key_expr_type, {}}, common_type_maybe_nullable, CastType::nonAccurate, {});

                        /// If we know the given range only contains one value, then we treat all functions as positive monotonic.
                        if (!single_point && !func_cast->hasInformationAboutMonotonicity())
                            return false;
                        chain.push_back(func_cast);
                    }
                }
            }

            /// Transformed constant must weaken the condition, for example "x > 5" must weaken to "round(x) >= 5"
            if (is_constant_transformed)
            {
                if (func_name == "less")
                    func_name = "lessOrEquals";
                else if (func_name == "greater")
                    func_name = "greaterOrEquals";
            }
        }
        else
            return false;

        const auto atom_it = atom_map.find(func_name);

        out.key_column = key_column_num;
        out.monotonic_functions_chain = std::move(chain);

        return atom_it->second(out, const_value);
    }
    else if (getConstant(node, block_with_constants, const_value, const_type))
    {
        /// For cases where it says, for example, `WHERE 0 AND something`

        if (const_value.getType() == Field::Types::UInt64)
        {
            out.type = const_value.safeGet<UInt64>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Int64)
        {
            out.type = const_value.safeGet<Int64>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Float64)
        {
            out.type = const_value.safeGet<Float64>() != 0.0 ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Bool)
        {
            out.type = const_value.safeGet<bool>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
    }
    return false;
}

bool IcebergKeyCondition::tryParseLogicalOperatorFromAST(const ASTFunction * func, RPNElement & out)
{
    /// Functions AND, OR, NOT.
    /// Also a special function `indexHint` - works as if instead of calling a function there are just parentheses
    /// (or, the same thing - calling the function `and` from one argument).
    const ASTs & args = func->arguments->children;

    if (func->name == "not")
    {
        if (args.size() != 1)
            return false;

        out.type = IcebergExpression::Type::NOT;
    }
    else
    {
        if (func->name == "and" || func->name == "indexHint")
            out.type = IcebergExpression::Type::AND;
        else if (func->name == "or")
            out.type = IcebergExpression::Type::OR;
        else
            return false;
    }

    return true;
}

IcebergExpression::NodePtr IcebergKeyCondition::getExpressionTree(bool skip_non_direct_column_ref) const
{
    auto combine = [](IcebergExpression::NodePtr left, IcebergExpression::NodePtr right, IcebergExpression::Type type)
    {
        /// Simplify operators with for one constant condition.
        if (type == IcebergExpression::Type::AND)
        {
            /// false AND right
            if (left->type == IcebergExpression::Type::FALSE)
                return left;

            /// left AND false
            if (right->type == IcebergExpression::Type::FALSE)
                return right;

            /// true AND right
            if (left->type == IcebergExpression::Type::TRUE)
                return right;

            /// left AND true
            if (right->type == IcebergExpression::Type::TRUE)
                return left;
        }

        if (type == IcebergExpression::Type::OR)
        {
            /// false OR right
            if (left->type == IcebergExpression::Type::FALSE)
                return right;

            /// left OR false
            if (right->type == IcebergExpression::Type::FALSE)
                return left;

            /// true OR right
            if (left->type == IcebergExpression::Type::TRUE)
                return left;

            /// left OR true
            if (right->type == IcebergExpression::Type::TRUE)
                return right;
        }

        auto new_node = IcebergExpression::newNode(type);
        new_node->children.push_back(std::move(left));
        new_node->children.push_back(std::move(right));

        return new_node;
    };

    std::vector<std::string_view> key_names(key_columns.size());

    for (const auto & key : key_columns)
        key_names[key.second] = key.first;

    auto print_wrapped_column = [&key_names](const RPNElement & element) -> String
    {
        WriteBufferFromOwnString buf;
        for (auto it = element.monotonic_functions_chain.rbegin(); it != element.monotonic_functions_chain.rend(); ++it)
        {
            buf << (*it)->getName() << "(";
            if (const auto * func = typeid_cast<const FunctionWithOptionalConstArg *>(it->get()))
            {
                if (func->getKind() == FunctionWithOptionalConstArg::Kind::LEFT_CONST)
                    buf << applyVisitor(FieldVisitorToString(), (*func->getConstArg().column)[0]) << ", ";
            }
        }

        buf << key_names[element.key_column];

        for (auto it = element.monotonic_functions_chain.rbegin(); it != element.monotonic_functions_chain.rend(); ++it)
        {
            if (const auto * func = typeid_cast<const FunctionWithOptionalConstArg *>(it->get()))
            {
                if (func->getKind() == FunctionWithOptionalConstArg::Kind::RIGHT_CONST)
                    buf << ", " << applyVisitor(FieldVisitorToString(), (*func->getConstArg().column)[0]);
            }
            buf << ")";
        }

        return buf.str();
    };

    std::vector<IcebergExpression::NodePtr> rpn_stack;

    for (const auto & element : rpn)
    {
        switch (element.type)
        {
            case IcebergExpression::Type::UNKNOWN:
            case IcebergExpression::Type::TRUE: {
                auto node = IcebergExpression::newNode(IcebergExpression::Type::TRUE);
                rpn_stack.push_back(std::move(node));
            }
            break;
            case IcebergExpression::Type::FALSE: {
                auto node = IcebergExpression::newNode(IcebergExpression::Type::FALSE);
                rpn_stack.push_back(std::move(node));
            }
            break;
            case IcebergExpression::Type::NOT: {
                auto node = std::move(rpn_stack.back());
                rpn_stack.pop_back();
                auto new_node = IcebergExpression::newNode(IcebergExpression::Type::NOT);
                new_node->children.push_back(std::move(node));
                rpn_stack.push_back(std::move(new_node));
            }
            break;
            case IcebergExpression::Type::AND:
            case IcebergExpression::Type::OR: {
                auto left = std::move(rpn_stack.back());
                rpn_stack.pop_back();

                auto right = std::move(rpn_stack.back());
                rpn_stack.pop_back();

                auto new_node = combine(std::move(left), std::move(right), element.type);
                rpn_stack.push_back(std::move(new_node));
            }
            break;
            case IcebergExpression::Type::IS_NULL:
            case IcebergExpression::Type::NOT_NULL:
            case IcebergExpression::Type::IS_NAN:
            case IcebergExpression::Type::NOT_NAN:
            case IcebergExpression::Type::LT:
            case IcebergExpression::Type::LT_EQ:
            case IcebergExpression::Type::GT:
            case IcebergExpression::Type::GT_EQ:
            case IcebergExpression::Type::EQ:
            case IcebergExpression::Type::NOT_EQ:
            case IcebergExpression::Type::IN:
            case IcebergExpression::Type::NOT_IN:
            case IcebergExpression::Type::STARTS_WITH:
            case IcebergExpression::Type::NOT_STARTS_WITH:
            case IcebergExpression::Type::RANGE:
            case IcebergExpression::Type::HAS_TERM:
            case IcebergExpression::Type::NOT_HAS_TERM:
            case IcebergExpression::Type::LIKE:
            case IcebergExpression::Type::NOT_LIKE:
            case IcebergExpression::Type::ARRAY_CONTAINS: {
                if (skip_non_direct_column_ref && !element.monotonic_functions_chain.empty())
                {
                    auto node = IcebergExpression::newNode(IcebergExpression::Type::TRUE);
                    rpn_stack.push_back(std::move(node));
                }
                else
                {
                    auto node = IcebergExpression::newNode(IcebergExpression::Type::LEAF);
                    node->element = IcebergExpression::newElement(element.type, print_wrapped_column(element), element.value);
                    rpn_stack.push_back(std::move(node));
                }
            }
            break;
            case IcebergExpression::Type::LEAF:
            case IcebergExpression::Type::COUNT:
            case IcebergExpression::Type::COUNT_STAR:
            case IcebergExpression::Type::MAX:
            case IcebergExpression::Type::MIN:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected node type: {}", IcebergExpression::typeToString(element.type));
        }
    }

    if (rpn_stack.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected stack size in IcebergCondition::checkInRange");

    return std::move(rpn_stack.back());
}
}
