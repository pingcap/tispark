package com.pingcap.tispark

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan


object TiCoprocessorOperation {
  type ReturnType = (Seq[NamedExpression], // partial grouping expressions
    Seq[NamedExpression], // partial aggregation expressions
    Seq[NamedExpression], // final grouping expressions
    Seq[NamedExpression], // final aggregation expressions
    LogicalPlan) // child plan

  private def isTiUnsupportedAggregate(agg: AggregateExpression): Boolean = {
    // TODO: Add test logic
    false
  }

  private def isTiUnsupportedAggExpr(aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.flatMap(expr => expr.collect { case agg: AggregateExpression => agg })
      .exists(agg => isTiUnsupportedAggregate(agg))
  }

  /**
    * For aggregation plans like
    * select sum(expr1) + expr3, count(expr2), col1 from t1 group by col1, col2
    * we turn it into
    * select sum(par_agg1) + expr3, count(par_agg2) from (
    *   select sum(expr1) as par_agg1, count(expr2) as par_agg2, col1, col2 from xx group by col1, col2
    * ) group by col1, col2
    * The sub-query above is pushed into TiKV's coprocessor
    */
  private def planForCoprocessor(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan): Option[ReturnType] = {
    None
  }

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    case logical.Aggregate(groupExprs, aggExprs, child) if (!isTiUnsupportedAggExpr(aggExprs)) => {
      planForCoprocessor(groupExprs, aggExprs, child)
    }
    case _ => None
  }
}
