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
import org.apache.spark.sql.sources.CatalystSource
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

      case LogicalRelation(_: CatalystSource, _, _) => true

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

  def planToSelectRequest(plan: LogicalPlan, selReq: TiSelectRequest, source: TiDBRelation)
  : TiSelectRequest = {

    plan match {
      case PhysicalAggregation(
      groupingExpressions, aggregateExpressions, _, child) =>
        aggregateExpressions.foreach(aggExpr =>
          aggExpr.aggregateFunction match {
          case Average(_) =>
            assert(false, "Should never be here")
          case Sum(BasicExpression(arg)) => {
            selReq.addAggregate(new TiSum(arg),
                                fromSparkType(aggExpr.aggregateFunction.dataType))
          }
          case Count(args) => {
            val tiArgs = args.flatMap(BasicExpression.convertToTiExpr)
            selReq.addAggregate(new TiCount(tiArgs: _*))
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
        planToSelectRequest(child, selReq, source)

        // matching a projection and filter directly on the top of CatalystSource (TiDB source)
      case PhysicalOperation(projectList, filters, rel@LogicalRelation(_: CatalystSource, _, _)) if rel ne plan => {
        // extract all attribute references for column pruning
        val projectSet = AttributeSet(projectList.flatMap(_.references))

        projectSet
          .map(ref => TiColumnRef.create(ref.name))
          .foreach(selReq.addField)

        val tiFilters = filters
          .map(expr => expr match { case BasicExpression(expr) => expr })

        val scanBuilder = new ScanBuilder
        val pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
        val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
                                             pkIndex, source.table)

        selReq.addRanges(scanPlan.getKeyRanges)
        scanPlan.getFilters.toList.map(selReq.addWhere)

        planToSelectRequest(rel, selReq, source)
      }

      case logical.Limit(IntegerLiteral(_), logical.Sort(_, true, child)) =>
        // TODO: fill builder with value
        planToSelectRequest(child, selReq, source)

      case logical.Limit(IntegerLiteral(_),
      logical.Project(_, logical.Sort(_, true, child))) =>
        // TODO: fill builder with value
        planToSelectRequest(child, selReq, source)

      case logical.Limit(IntegerLiteral(_), child) =>
        // TODO: fill builder with value
        planToSelectRequest(child, selReq, source)

        // End of recursive traversal
      case rel@LogicalRelation(source: CatalystSource, _, _) => {
        // Append full range
        if (selReq.getRanges.isEmpty) {
          val scanBuilder = new ScanBuilder
          val pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(source.tableInfo)
          val scanPlan = scanBuilder.buildScan(new util.ArrayList(), pkIndex, source.tableInfo)
          selReq.addRanges(scanPlan.getKeyRanges)
        }
        if (selReq.getFields.isEmpty &&
            selReq.getGroupByItems.isEmpty &&
            selReq.getAggregates.isEmpty) {
          // no aggregation and projection, take table relation field as columns
          rel.output.foreach(col => selReq.addField(TiColumnRef.create(col.name)))
        }
        selReq.setTableInfo(source.tableInfo)
      }

      case _ => selReq
    }
  }

}
