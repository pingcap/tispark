package com.pingcap.tispark


import com.pingcap.tikv.expression.{TiByItem, TiColumnRef}
import com.pingcap.tikv.meta.TiSelectRequest
import com.pingcap.tikv.types.{BytesType, DecimalType, IntegerType}
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.CatalystSource
import org.apache.spark.sql.types.DataType


object TiUtils {
  type TiSum = com.pingcap.tikv.expression.aggregate.Sum
  type TiCount = com.pingcap.tikv.expression.aggregate.Count
  type TiMin = com.pingcap.tikv.expression.aggregate.Min
  type TiMax = com.pingcap.tikv.expression.aggregate.Max
  type TiDataType = com.pingcap.tikv.types.DataType


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
  def toSparkDataType(tp: TiDataType): DataType = {
    tp match {
      case _: BytesType => sql.types.StringType
      case _: IntegerType => sql.types.LongType
      case _: DecimalType => sql.types.DoubleType
    }
  }

  def planToSelectRequest(plan: LogicalPlan, selReq: TiSelectRequest)
  : TiSelectRequest = {

    plan match {
      case PhysicalAggregation(
      groupingExpressions, aggregateExpressions, _, child) =>
        aggregateExpressions.foreach(aggExpr =>
          aggExpr.aggregateFunction match {
          case Average(_) =>
            assert(false, "Should never be here")
          case Sum(BasicExpression(arg)) => {
            selReq.addAggregate(new TiSum(arg))
          }
          case Count(BasicExpression(arg)) => {
            selReq.addAggregate(new TiCount(arg))
          }
          case Min(BasicExpression(arg)) => {
            selReq.addAggregate(new TiMin(arg))
          }
          case Max(BasicExpression(arg)) => {
            selReq.addAggregate(new TiMax(arg))
          }
          case _ =>
        })
        groupingExpressions.foreach(groupItem =>
          groupItem match {
            case BasicExpression(byExpr) =>
              selReq.addGroupByItem(TiByItem.create(byExpr, false))
            case _ =>
          }
        )
        planToSelectRequest(child, selReq)

      case PhysicalOperation(projectList, filters, child) if child ne plan => {
        // Assume project list should be all simple AttributeReference
        val projectSet = AttributeSet(projectList.flatMap(_.references))

        projectSet
          .map(ref => TiColumnRef.create(ref.name))
          .foreach(selReq.addField)

        filters
          .map(expr => expr match {
            case BasicExpression(expr) => expr
          }).foreach(selReq.addWhere)

        planToSelectRequest(child, selReq)
      }

      case logical.Limit(IntegerLiteral(_), logical.Sort(_, true, child)) =>
        // TODO: fill builder with value
        planToSelectRequest(child, selReq)

      case logical.Limit(IntegerLiteral(_),
      logical.Project(_, logical.Sort(_, true, child))) =>
        // TODO: fill builder with value
        planToSelectRequest(child, selReq)

      case logical.Limit(IntegerLiteral(_), child) =>
        // TODO: fill builder with value
        planToSelectRequest(child, selReq)

        // End of recursive traversal
      case LogicalRelation(source: CatalystSource, _, _) =>
        selReq.setTableInfo(source.tableInfo)

      case _ => selReq
    }
  }

}
