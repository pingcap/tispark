package com.pingcap.tispark

import com.pingcap.tikv.`type`.{DecimalType, FieldType, LongType, StringType}
import com.pingcap.tikv.SelectBuilder
import com.pingcap.tikv.expression.{TiAggregateFunction, TiColumnRef}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Expression, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.CatalystSource
import org.apache.spark.sql.types.DataType


object TiUtils {
  def isSupportedLogicalPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalAggregation(
      groupingExpressions, aggregateExpressions, _, child) =>
        !aggregateExpressions.exists(expr => !isSupportedAggregate(expr)) &&
          !groupingExpressions.exists(expr => !isSupportedGroupingExpr(expr)) &&
          isSupportedLogicalPlan(child)

      case PhysicalOperation(projectList, filters, child) if child ne plan =>
        isSupportedPhysicalOperation(plan, projectList, filters, child)

      case logical.ReturnAnswer(rootPlan) => rootPlan match {
        case logical.Limit(IntegerLiteral(_), logical.Sort(_, true, child)) =>
          isSupportedPlanWithDistinct(child)
        case logical.Limit(IntegerLiteral(_),
        logical.Project(_, logical.Sort(_, true, child))) =>
          isSupportedPlanWithDistinct(child)
        case logical.Limit(IntegerLiteral(_), child) =>
          isSupportedPlanWithDistinct(child)
        case _ => false
      }

      case LogicalRelation(_: CatalystSource, _, _) => true

      case _ => false
    }
  }

  private def isSupportedPhysicalOperation(currentPlan: LogicalPlan,
                                           projectList: Seq[NamedExpression],
                                           filterList: Seq[Expression],
                                           child: LogicalPlan): Boolean = {
    // It seems Spark return the plan itself if no match instead of fail
    // So do a test avoiding unlimited recursion
    !projectList.exists(expr => !isSupportedProjection(expr)) &&
      !filterList.exists(expr => !isSupportedFilter(expr)) &&
      isSupportedLogicalPlan(child)
  }

  private def isSupportedPlanWithDistinct(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(projectList, filters, child) if child ne plan =>
        isSupportedPhysicalOperation(plan, projectList, filters, child)
      case _: TiDBRelation => true
      case _ => false
    }
  }

  private def isSupportedAggregate(aggExpr: AggregateExpression): Boolean = {
    aggExpr.aggregateFunction match {
      case Average(_) | Sum(_) | Count(_) | Min(_) | Max(_) =>
        !aggExpr.isDistinct &&
          !aggExpr.aggregateFunction
            .children.exists(expr => !isSupportedBasicExpression(expr))
      case _ => false
    }
  }

  private def isSupportedBasicExpression(expr: Expression) = {
    expr match {
      case BasicExpression(_) => true
      case _ => false
    }
  }

  private def isSupportedProjection(expr: Expression): Boolean = {
    expr.find(child => !isSupportedBasicExpression(child)).isEmpty
  }

  private def isSupportedFilter(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // 1. if contains UDF / functions that cannot be folded
  private def isSupportedGroupingExpr(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // convert tikv-java client FieldType to Spark DataType
  def toSparkDataType(tp:FieldType): DataType = {
    tp match {
      case _: StringType => sql.types.StringType
      case _: LongType => sql.types.LongType
      case _: DecimalType => sql.types.DoubleType
    }
  }

  def coprocessorReqToBytes(plan: LogicalPlan,
                            builder: SelectBuilder)
  : SelectBuilder = {
    plan match {
      case PhysicalAggregation(
      groupingExpressions, aggregateExpressions, _, child) =>
        aggregateExpressions.foreach(aggExpr => aggExpr.aggregateFunction match {
          case Average(_) =>
            assert(false, "Should never be here")
          case Sum(_) =>
             builder.addAggregates(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Sum, TiColumnRef.create(aggExpr.resultAttribute.name, builder.table)))
          case Count(_) =>
            builder.addAggregates(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Count, TiColumnRef.create(aggExpr.resultAttribute.name, builder.table)))
          case Min(_) => builder.addAggregates(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Min))
            builder.addAggregates(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Min, TiColumnRef.create(aggExpr.resultAttribute.name, builder.table)))
          case Max(_) => builder.addAggregates(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Max))
            builder.addAggregates(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Max, TiColumnRef.create(aggExpr.resultAttribute.name, builder.table)))
        })
        coprocessorReqToBytes(child, builder)

      case PhysicalOperation(projectList, filters, child) if child ne plan =>
        // TODO: fill builder with value
        coprocessorReqToBytes(child, builder)

      case logical.Limit(IntegerLiteral(_), logical.Sort(_, true, child)) =>
        // TODO: fill builder with value
        coprocessorReqToBytes(child, builder)

      case logical.Limit(IntegerLiteral(_),
      logical.Project(_, logical.Sort(_, true, child))) =>
        // TODO: fill builder with value
        coprocessorReqToBytes(child, builder)

      case logical.Limit(IntegerLiteral(_), child) =>
        // TODO: fill builder with value
        coprocessorReqToBytes(child, builder)

        // End of recursive traversal
      case LogicalRelation(_: CatalystSource, _, _) => builder
    }
  }

}
