package com.pingcap.tispark


import java.util

import com.pingcap.tikv.expression.{TiByItem, TiColumnRef}
import com.pingcap.tikv.meta.{TiIndexInfo, TiSelectRequest}
import com.pingcap.tikv.predicates.ScanBuilder
import com.pingcap.tikv.types._
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConversions
import scala.collection.JavaConversions._


object TiUtils {
  type TiSum = com.pingcap.tikv.expression.aggregate.Sum
  type TiCount = com.pingcap.tikv.expression.aggregate.Count
  type TiMin = com.pingcap.tikv.expression.aggregate.Min
  type TiMax = com.pingcap.tikv.expression.aggregate.Max
  type TiFirst = com.pingcap.tikv.expression.aggregate.First
  type TiDataType = com.pingcap.tikv.types.DataType
  type TiTypes = com.pingcap.tikv.types.Types


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

      case LogicalRelation(_: TiDBRelation, _, _) => true

      case _ => false
    }
  }

  def isSupportedPhysicalOperation(currentPlan: LogicalPlan,
                                           projectList: Seq[NamedExpression],
                                           filterList: Seq[Expression],
                                           child: LogicalPlan): Boolean = {
    // It seems Spark return the plan itself if no match instead of fail
    // So do a test avoiding unlimited recursion
    !projectList.exists(expr => !isSupportedProjection(expr)) &&
      !filterList.exists(expr => !isSupportedFilter(expr)) &&
      isSupportedLogicalPlan(child)
  }

  def isSupportedPlanWithDistinct(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(projectList, filters, child) if child ne plan =>
        isSupportedPhysicalOperation(plan, projectList, filters, child)
      case _: TiDBRelation => true
      case _ => false
    }
  }

  def isSupportedAggregate(aggExpr: AggregateExpression): Boolean = {
    aggExpr.aggregateFunction match {
      case Average(_) | Sum(_) | Count(_) | Min(_) | Max(_) =>
        !aggExpr.isDistinct &&
          !aggExpr.aggregateFunction
            .children.exists(expr => !isSupportedBasicExpression(expr))
      case _ => false
    }
  }

  def isSupportedBasicExpression(expr: Expression) = {
    !BasicExpression.convertToTiExpr(expr).isEmpty
  }

  def isSupportedProjection(expr: Expression): Boolean = {
    expr.find(child => !isSupportedBasicExpression(child)).isEmpty
  }

  def isSupportedFilter(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // 1. if contains UDF / functions that cannot be folded
  def isSupportedGroupingExpr(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // convert tikv-java client FieldType to Spark DataType
  def toSparkDataType(tp: TiDataType): DataType = {
    tp match {
      case _: RawBytesType => sql.types.BinaryType
      case _: BytesType => sql.types.StringType
      case _: IntegerType => sql.types.LongType
      case _: DecimalType => sql.types.DoubleType
      case _: TimestampType => sql.types.TimestampType
    }
  }

  def fromSparkType(tp: DataType): TiDataType = {
    tp match {
      case _: sql.types.BinaryType => DataTypeFactory.of(Types.TYPE_BLOB)
      case _: sql.types.StringType => DataTypeFactory.of(Types.TYPE_VARCHAR)
      case _: sql.types.LongType => DataTypeFactory.of(Types.TYPE_LONG)
      case _: sql.types.DoubleType => DataTypeFactory.of(Types.TYPE_NEW_DECIMAL)
      case _: sql.types.TimestampType => DataTypeFactory.of(Types.TYPE_DATE)
    }
  }

  def projectFilterToSelectRequest(projects: Seq[NamedExpression],
                                   filters: Seq[Expression],
                                   source: TiDBRelation): TiSelectRequest = {
    val selReq: TiSelectRequest = new TiSelectRequest
    val tiFilters = filters.map(expr => expr match { case BasicExpression(expr) => expr })
    val scanBuilder = new ScanBuilder
    val pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
    val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
      pkIndex, source.table)

    selReq.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.toList.map(selReq.addWhere)

    selReq
  }

}
