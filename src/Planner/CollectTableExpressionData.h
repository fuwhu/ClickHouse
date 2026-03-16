#pragma once

#include <Planner/PlannerContext.h>

#include <Analyzer/IQueryTreeNode.h>

namespace DB
{

/** Collect table expression data for query node.
  * Collected table expression data is registered in planner context.
  *
  * ALIAS table column nodes are registered in table expression data and replaced in query tree with inner alias expression.
  */
void collectTableExpressionData(QueryTreeNodePtr & query_node, PlannerContextPtr & planner_context);

/** Collect source columns for expression node.
  * Collected source columns are registered in planner context.
  *
  * ALIAS table column nodes are registered in table expression data and replaced in query tree with inner alias expression.
  */
void collectSourceColumns(QueryTreeNodePtr & expression_node, PlannerContextPtr & planner_context, bool keep_alias_columns = true);

/** Collect WHERE and GROUP BY columns for query node.
  * Collected column information is registered in global planner context's storage_to_columns_info.
  * This mapping tracks columns used in WHERE/PREWHERE/HAVING/GROUP BY clauses per source storage.
  */
void collectWhereAndGroupByColumns(QueryTreeNodePtr & query_node, PlannerContextPtr & planner_context);

}
