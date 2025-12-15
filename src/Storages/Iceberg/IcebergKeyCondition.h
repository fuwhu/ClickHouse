#pragma once

#include "IcebergCommon.h"
#include "Interpreters/ActionsDAG.h"

#include <optional>
#include <Core/SortDescription.h>
#include <Parsers/ASTExpressionList.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/MergeTree/RPNBuilder.h>


namespace DB
{

class ASTFunction;
class Context;
class IFunction;
// using FunctionBasePtr = std::shared_ptr<IFunctionBase>;
class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;
struct ActionsDAGWithInversionPushDown;
class MergeTreeSetIndex;

class IcebergKeyCondition
{
public:
    IcebergKeyCondition(
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        const Names & key_column_names,
        const ExpressionActionsPtr & key_expr,
        bool single_point_ = false,
        bool strict_ = false);
    
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

        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_IN_RANGE,
            FUNCTION_NOT_IN_RANGE,
            FUNCTION_IN_SET,
            FUNCTION_NOT_IN_SET,
            FUNCTION_IS_NULL,
            FUNCTION_IS_NOT_NULL,
            /// Special for space-filling curves.
            /// For example, if key is mortonEncode(x, y),
            /// and the condition contains its arguments, e.g.:
            ///   x >= 10 AND x <= 20 AND y >= 20 AND y <= 30,
            /// this expression will be analyzed and then represented by following:
            ///   args in hyperrectangle [10, 20] × [20, 30].
            FUNCTION_ARGS_IN_HYPERRECTANGLE,
            /// Special for pointInPolygon to utilize minmax indices.
            /// For example: pointInPolygon((x, y), [(0, 0), (0, 2), (2, 2), (2, 0)])
            FUNCTION_POINT_IN_POLYGON,
            /// Can take any value.
            FUNCTION_UNKNOWN,
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        IcebergExpression::Type type = IcebergExpression::Type::UNKNOWN;

        Function function = FUNCTION_UNKNOWN;

        Field value;
        size_t key_column = 0;

        /// Whether to relax the key condition (e.g., for LIKE queries without a perfect prefix).
        bool relaxed = false;

        /// If the key_column is a space filling curve, e.g. mortonEncode(x, y),
        /// we will analyze expressions of its arguments (x and y) similarly how we do for a normal key columns,
        /// and this designates the argument number (0 for x, 1 for y):
        std::optional<size_t> argument_num_of_space_filling_curve;

        /// For FUNCTION_IN_SET, FUNCTION_NOT_IN_SET
        using MergeTreeSetIndexPtr = std::shared_ptr<const MergeTreeSetIndex>;
        MergeTreeSetIndexPtr set_index;

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
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        bool assume_function_monotonicity = false);

    bool isKeyPossiblyWrappedByMonotonicFunctionsImpl(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        std::optional<size_t> & out_argument_num_of_space_filling_curve,
        DataTypePtr & out_key_column_type,
        std::vector<RPNBuilderFunctionTreeNode> & out_functions_chain);

    bool extractMonotonicFunctionsChainFromKey(
        ContextPtr context,
        const String & expr_name,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        MonotonicFunctionsChain & out_functions_chain,
        std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const;
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
        std::function<bool(const IFunctionBase &, const IDataType &)> always_monotonic) const;

    bool canConstantBeWrappedByMonotonicFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    bool canConstantBeWrappedByFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_column_type,
        Field & out_value,
        DataTypePtr & out_type);

    bool canConstantBeWrappedByMonotonicFunctions(
        const ASTPtr & node, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type);

    bool canConstantBeWrappedByFunctions(
        const ASTPtr & ast, size_t & out_key_column_num, DataTypePtr & out_key_column_type, Field & out_value, DataTypePtr & out_type);

    bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out); 

    /// Checks if node is a subexpression of any of key columns expressions,
    /// wrapped by deterministic functions, and if so, returns `true`, and
    /// specifies key column position / type. Besides that it produces the
    /// chain of functions which should be executed on set, to transform it
    /// into key column values.
    bool canSetValuesBeWrappedByFunctions(
        const RPNBuilderTreeNode & node,
        size_t & out_key_column_num,
        DataTypePtr & out_key_res_column_type,
        MonotonicFunctionsChain & out_functions_chain);

    /// If it's possible to make an RPNElement
    /// that will filter values (possibly tuples) by the content of 'prepared_set',
    /// do it and return true.
    bool tryPrepareSetIndex(
        const RPNBuilderFunctionTreeNode & func,
        RPNElement & out,
        size_t & out_key_column_num,
        bool & allow_constant_transformation,
        bool & is_constant_transformed);   

    /// If query has no filter, rpn will has one element with unknown function.
    /// This flag identify whether there are filters.
    bool has_filter;

    RPN rpn;

    ColumnIndices key_columns;
    std::vector<size_t> key_indices;
    /// Expression which is used for key condition.
    const ExpressionActionsPtr key_expr;
    /// All intermediate columns are used to calculate key_expr.
    const NameSet key_subexpr_names;

    NameSet array_joined_column_names;
    // PreparedSets prepared_sets;

    // If true, always allow key_expr to be wrapped by function
    bool single_point;

    /// Determines if a function maintains monotonicity.
    /// Currently only does special checks for toDateTime monotonicity.
    bool isFunctionReallyMonotonic(const IFunctionBase & func, const IDataType & arg_type) const;
    
    // If true, do not use always_monotonic information to transform constants
    bool strict;

    /// Holds the result of (setting.date_time_overflow_behavior == DateTimeOverflowBehavior::Ignore)
    /// Used to check toDateTime monotonicity.
    bool date_time_overflow_behavior_ignore;

    /// If true, this key condition is relaxed. When a key condition is relaxed, it
    /// is considered weakened. This is because keys may not always align perfectly
    /// with the condition specified in the query, and the aim is to enhance the
    /// usefulness of different types of key expressions across various scenarios.
    ///
    /// For instance, in a scenario with one granule of key column toDate(a), where
    /// the hyperrectangle is toDate(a) ∊ [x, y], the result of a ∊ [u, v] can be
    /// deduced as toDate(a) ∊ [toDate(u), toDate(v)] due to the monotonic
    /// non-decreasing nature of the toDate function. Similarly, for a ∊ (u, v), the
    /// transformed outcome remains toDate(a) ∊ [toDate(u), toDate(v)] as toDate
    /// does not strictly follow a monotonically increasing transformation. This is
    /// one of the main use case about key condition relaxation.
    ///
    /// During the KeyCondition::checkInRange process, relaxing the key condition
    /// can lead to a loosened result. For example, when transitioning from (u, v)
    /// to [u, v], if a key is within the range [u, u], BoolMask::can_be_true will
    /// be true instead of false, causing us to not skip this granule. This behavior
    /// is acceptable as we can still filter it later on. Conversely, if the key is
    /// within the range [u, v], BoolMask::can_be_false will be false instead of
    /// true, indicating a stricter condition where all elements of the granule
    /// satisfy the key condition. Hence, when the key condition is relaxed, we
    /// cannot rely on BoolMask::can_be_false. One significant use case of
    /// BoolMask::can_be_false is in trivial count optimization.
    ///
    /// Now let's review all the cases of key condition relaxation across different
    /// atom types.
    ///
    /// 1. Not applicable: ALWAYS_FALSE, ALWAYS_TRUE, FUNCTION_NOT,
    /// FUNCTION_AND, FUNCTION_OR.
    ///
    /// These atoms are either never relaxed or are relaxed by their children.
    ///
    /// 2. Constant transformed: FUNCTION_IN_RANGE, FUNCTION_NOT_IN_RANGE,
    /// FUNCTION_IS_NULL. FUNCTION_IS_NOT_NULL, FUNCTION_IN_SET (1 element),
    /// FUNCTION_NOT_IN_SET (1 element)
    ///
    /// These atoms are relaxed only when the associated constants undergo
    /// transformation by monotonic functions, as illustrated in the example
    /// mentioned earlier.
    ///
    /// 3. Always relaxed: FUNCTION_UNKNOWN, FUNCTION_IN_SET (>1 elements),
    /// FUNCTION_NOT_IN_SET (>1 elements), FUNCTION_ARGS_IN_HYPERRECTANGLE
    ///
    /// These atoms are always considered relaxed for the sake of implementation
    /// simplicity, as there may be "gaps" within the atom's hyperrectangle that the
    /// granule's hyperrectangle may or may not intersect.
    ///
    /// NOTE: we also need to examine special functions that generate atoms. For
    /// example, the `match` function can produce a FUNCTION_IN_RANGE atom based
    /// on a given regular expression, which is relaxed for simplicity.
    bool relaxed = false;
};
}
