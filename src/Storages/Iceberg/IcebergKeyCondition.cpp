#include "IcebergKeyCondition.h"

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/getLeastSupertype.h>
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
#include <Parsers/queryToString.h>
#include <Storages/KeyDescription.h>
#include <Common/FieldVisitorToString.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Common/typeid_cast.h>

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

/// A dictionary containing actions to the corresponding functions to turn them into `RPNElement`
const IcebergKeyCondition::AtomMap IcebergKeyCondition::atom_map{
    {"notEquals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_EQ;
         out.value = value;
         return true;
     }},
    {"equals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::EQ;
         out.value = value;
         return true;
     }},
    {"less",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::LT;
         out.value = value;
         return true;
     }},
    {"greater",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::GT;
         out.value = value;
         return true;
     }},
    {"lessOrEquals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::LT_EQ;
         out.value = value;
         return true;
     }},
    {"greaterOrEquals",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::GT_EQ;
         out.value = value;
         return true;
     }},
    {"in",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.value = value;
         return true;
     }},
    {"notIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.value = value;
         return true;
     }},
    {"globalIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.value = value;
         return true;
     }},
    {"globalNotIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.value = value;
         return true;
     }},
    {"nullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.value = value;
         return true;
     }},
    {"notNullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.value = value;
         return true;
     }},
    {"globalNullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::IN;
         out.value = value;
         return true;
     }},
    {"globalNotNullIn",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::NOT_IN;
         out.value = value;
         return true;
     }},
    {"empty",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::EQ;
         out.value = "";
         return true;
     }},
    {"notEmpty",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::NOT_EQ;
         out.value = "";
         return true;
     }},
    {"like",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::LIKE;
         out.value = value;

         return true;
     }},
    {"notLike",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::NOT_LIKE;
         out.value = value;

         return true;
     }},
    {"startsWith",
     [](RPNElement & out, const Field & value)
     {
         if (value.getType() != Field::Types::String)
             return false;

         out.type = IcebergExpression::Type::STARTS_WITH;
         out.value = value;

         return true;
     }},
    {"isNotNull",
     [](RPNElement & out, const Field &)
     {
         out.type = IcebergExpression::Type::NOT_NULL;
         // isNotNull means (-Inf, +Inf), which is the default Range
         return true;
     }},
    {"isNull",
     [](RPNElement & out, const Field &)
     {
         out.type = IcebergExpression::Type::IS_NULL;
         // isNull means +Inf (NULLS_LAST) or -Inf (NULLS_FIRST),
         // which is equivalent to not in Range (-Inf, +Inf)
         return true;
     }},
    {"hasToken",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::HAS_TERM;
         out.value = value;

         return true;
     }},
    {"has",
     [](RPNElement & out, const Field & value)
     {
         out.type = IcebergExpression::Type::ARRAY_CONTAINS;
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

    const auto expr_for_constant_folding = ExpressionAnalyzer(query, syntax_analyzer_result, context).getConstActions();

    expr_for_constant_folding->execute(result);

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
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const Names & key_column_names,
    const ExpressionActionsPtr & key_expr_,
    bool single_point_,
    bool strict_)
    : key_expr(key_expr_)
    , key_subexpr_names(getAllSubexpressionNames(*key_expr))
    , prepared_sets(query_info.sets)
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

    for (const auto & [name, _] : query_info.syntax_analyzer_result->array_join_result_to_source)
        array_joined_columns.insert(name);

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

/// The case when arguments may have types different than in the primary key.
static std::pair<Field, DataTypePtr>
applyFunctionForFieldOfUnknownType(const FunctionBasePtr & func, const DataTypePtr & arg_type, const Field & arg_value)
{
    ColumnsWithTypeAndName arguments{{arg_type->createColumnConst(1, arg_value), arg_type, "x"}};
    DataTypePtr return_type = func->getResultType();

    auto col = func->execute(arguments, return_type, 1);

    Field result = (*col)[0];

    return {std::move(result), std::move(return_type)};
}

/// Same as above but for binary operators
static std::pair<Field, DataTypePtr> applyBinaryFunctionForFieldOfUnknownType(
    const FunctionOverloadResolverPtr & func,
    const DataTypePtr & arg_type,
    const Field & arg_value,
    const DataTypePtr & arg_type2,
    const Field & arg_value2)
{
    ColumnsWithTypeAndName arguments{
        {arg_type->createColumnConst(1, arg_value), arg_type, "x"}, {arg_type2->createColumnConst(1, arg_value2), arg_type2, "y"}};

    FunctionBasePtr func_base = func->build(arguments);

    DataTypePtr return_type = func_base->getResultType();

    auto col = func_base->execute(arguments, return_type, 1);

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
    std::function<bool(IFunctionBase &, const IDataType &)> always_monotonic) const
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
                                func->function_builder, left_arg_type, left_arg_value, const_type, const_value);
                        }
                        else
                        {
                            auto right_arg_type = right->result_type;
                            auto right_arg_value = (*right->column)[0];
                            std::tie(const_value, const_type) = applyBinaryFunctionForFieldOfUnknownType(
                                func->function_builder, const_type, const_value, right_arg_type, right_arg_value);
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

bool IcebergKeyCondition::canConstantBeWrappedByMonotonicFunctions(
    const ASTPtr & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type)
{
    String expr_name = node->getColumnNameWithoutAlias();

    if (array_joined_columns.count(expr_name))
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
        [](IFunctionBase & func, const IDataType & type)
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

    if (array_joined_columns.count(expr_name))
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
        [](IFunctionBase & func, const IDataType &) { return func.isDeterministic(); });
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

    if (array_joined_columns.count(name))
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
        throw Exception(
            "Key expression contains comparison between inconvertible types: " + desired_type->getName() + " and " + src_type->getName()
                + " inside " + queryToString(node),
            ErrorCodes::BAD_TYPE_OF_FIELD);
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
                throw Exception("`key_column_num` wasn't initialized. It is a bug.", ErrorCodes::LOGICAL_ERROR);
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
                throw Exception("`key_column_num` wasn't initialized. It is a bug.", ErrorCodes::LOGICAL_ERROR);

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
                    DataTypePtr common_type = tryGetLeastSupertype({key_expr_type_not_null, const_type});
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
                        ColumnsWithTypeAndName arguments{
                            {nullptr, key_expr_type, ""},
                            {DataTypeString().createColumnConst(1, common_type_maybe_nullable->getName()), common_type_maybe_nullable, ""}};
                        FunctionOverloadResolverPtr func_builder_cast = CastInternalOverloadResolver<CastType::nonAccurate>::createImpl();
                        auto func_cast = func_builder_cast->build(arguments);

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
            out.type = const_value.safeGet<Float64>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
            return true;
        }
        else if (const_value.getType() == Field::Types::Bool)
        {
            out.type = const_value.get<bool>() ? IcebergExpression::Type::TRUE : IcebergExpression::Type::FALSE;
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
