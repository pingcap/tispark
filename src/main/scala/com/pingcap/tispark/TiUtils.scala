package com.pingcap.tispark

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


object TiUtils {

  def isSupportedLogicalPlan(plan: LogicalPlan): Boolean = {
    // We need to check:
    // 1. Aggregates:
    //      if distinct exists;
    //      if non-supported aggregates exists
    //      if non-supported expression as aggregation argument
    //      if adjacent with Relation, or with Projection and Filter
    //      if non-supported grouping expressions
    // 2. Filter
    //      if non-supported expression as aggregation argument
    //      if not on the top of relation
    // 3. Limit
    //      if not on the top of relation or with projection and filters
    // 4. Projection and filter
    //      if not on the top of relation
    plan match {
      case PhysicalAggregation(
      groupingExpressions, aggregateExpressions, _, child) =>
        aggregateExpressions.exists(expr => !isSupportedAggregate(expr)) ||
          groupingExpressions.exists(expr => !isSupportedGroupingExpr(expr)) ||
          !isSupportedLogicalPlan(child)
      case PhysicalOperation(projectList, filters, child) =>
        projectList.exists(expr => !isSupportedProjection(expr)) ||
          filters.exists(expr => !isSupportedFilter(expr)) ||
          !isSupportedLogicalPlan(child)
    }
  }

  private def isSupportedAggregate(aggExpr: AggregateExpression): Boolean = {
    aggExpr.aggregateFunction match {
      case Average(_) | Sum(_) | Count(_) | Min(_) | Max(_) =>
        aggExpr.isDistinct ||
          !aggExpr.aggregateFunction.find(expr => isSupportedBasicExpression(expr)).isEmpty
      case _ => false
    }
  }

  private def isSupportedBasicExpression(expr: Expression) = true

  private def isSupportedProjection(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  private def isSupportedFilter(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // 1. if contains UDF / functions that cannot be folded
  private def isSupportedGroupingExpr(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

}
