package com.pingcap.tispark

import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation


// TODO: Consider limits and sorts
// TODO: Need to check it's a clean interception which only deal with TiDB related logic
// without interrupting other normal procedure
// Otherwise we cannot use TiContext for normal data read for other sources
object TiCoprocessorOperation {
  case class AggregateResult(partialGroupingExprs: Seq[NamedExpression],
                            partialAggregateExprs: Seq[NamedExpression],
                            finalGroupingExprs: Seq[NamedExpression],
                            finalAggregateExprs: Seq[NamedExpression])

  case class ProjectionFilterResult(projectionExprs: Seq[NamedExpression],
                                    filterExprs: Seq[Expression])

  type CoprocessorReq = (Option[AggregateResult], Option[ProjectionFilterResult], TiDBRelation)

  private def isTiUnsupportedAggregate(agg: AggregateExpression): Boolean = {
    // TODO: Add test logic
    false
  }

  private def isTiUnsupportedAggExpr(aggExprs: Seq[NamedExpression]): Boolean = {
    aggExprs.flatMap(expr => expr.collect { case agg: AggregateExpression => agg })
      .exists(agg => isTiUnsupportedAggregate(agg))
  }

  private def isTiUnsupportedBasicExpr(exprs: Expression): Boolean = {
    false
  }

  /**
    * Transform filters and projections that on the very top of relation
    * Please refer to [[org.apache.spark.sql.execution.datasources.DataSourceStrategy]]
    * for any future API change at Spark side
    */
  private def planBasicOperation(projections: Seq[NamedExpression],
                                 filters: Seq[Expression],
                                 t: LogicalRelation): Option[ProjectionFilterResult] = {
    if (projections.exists(expr => isTiUnsupportedBasicExpr(expr)) ||
        filters.exists(expr => isTiUnsupportedBasicExpr(expr))) None
    Some(ProjectionFilterResult(projections, filters))
  }

  /**
    * For aggregation plans like
    * select sum(expr1) + expr3, count(expr2), col1 from t1 group by col1, col2
    * we turn it into
    * select sum(par_agg1) + expr3, count(par_agg2) from (
    *   select sum(expr1) as par_agg1, count(expr2) as par_agg2, col1, col2 from xx group by col1, col2
    * ) group by col1, col2
    * The sub-query above is pushed into TiKV's coprocessor
    *
    */
  private def planAggregates(
    groupingExpressions: Seq[Expression],
    aggregateExpressions: Seq[NamedExpression],
    child: LogicalPlan): Option[CoprocessorReq] = {
    // Plan for aggregates
    val aggregateResult: Option[AggregateResult] = None


    // Plan for child if it's filters and projections
    // For now it's a valid coprocessor plan iff there's nothing between aggregates
    // and filters/projections
    child match {
      case PhysicalOperation(projects, filters, logicalRelation@LogicalRelation(t: TiDBRelation, _, _)) => {
        Some(aggregateResult, planBasicOperation(projects, filters, logicalRelation), t)
      }
      case _ => None
    }
  }

  def unapply(plan: LogicalPlan): Option[CoprocessorReq] = plan match {
    /**
      * Matches aggregates that fully supported by TiKV and transform into coprocessor plan
      */
    case logical.Aggregate(groupExprs, aggExprs, child) if (!isTiUnsupportedAggExpr(aggExprs)) => {
      planAggregates(groupExprs, aggExprs, child)
    }

    /**
      * Matches filters and projections supported by TiKV and transform into coprocessor plan
      */
    case PhysicalOperation(projects, filters, logicalRelation@LogicalRelation(t: TiDBRelation, _, _)) => {
      Some(None, planBasicOperation(projects, filters, logicalRelation), t)
    }
    case _ => None
  }
}
