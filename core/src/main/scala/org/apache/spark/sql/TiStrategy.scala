/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql

import com.pingcap.tikv.expression.scalar.TiScalarFunction
import java.time.ZonedDateTime

import com.pingcap.tikv.codec.IgnoreUnsupportedTypeException
import com.pingcap.tikv.expression
import com.pingcap.tikv.expression.{aggregate => _, _}
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType
import com.pingcap.tikv.predicates.ScanBuilder
import com.pingcap.tispark.TiUtils._
import com.pingcap.tispark.{BasicExpression, TiConfigConst, TiDBRelation, TiUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Cast, Divide, ExprId, Expression, IntegerLiteral, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{JavaConversions, mutable}

// TODO: Too many hacks here since we hijack the planning
// but we don't have full control over planning stage
// We cannot pass context around during planning so
// a re-extract needed for pushdown since
// a plan tree might have Join which causes a single tree
// have multiple plan to pushdown
class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf: SQLConf = context.conf
  def blacklist: ExpressionBlacklist = {
    val blacklistString = sqlConf.getConfString(TiConfigConst.UNSUPPORTED_PUSHDOWN_EXPR, "")
    new ExpressionBlacklist(blacklistString)
  }

  def typeBlackList: TypeBlacklist = {
    val blacklistString = sqlConf.getConfString(TiConfigConst.UNSUPPORTED_TYPES, "")
    new TypeBlacklist(blacklistString)
  }

  def allowAggregationPushDown(): Boolean = {
    sqlConf.getConfString(TiConfigConst.ALLOW_AGG_PUSHDOWN, "true").toBoolean
  }

  def allowIndexDoubleRead(): Boolean = {
    sqlConf.getConfString(TiConfigConst.ALLOW_INDEX_DOUBLE_READ, "false").toBoolean
  }

  def useStreamingProcess(): Boolean = {
    sqlConf.getConfString(TiConfigConst.COPROCESS_STREAMING, "false").toBoolean
  }

  def timeZoneOffset(): Int = {
    sqlConf
      .getConfString(
        TiConfigConst.KV_TIMEZONE_OFFSET,
        String.valueOf(ZonedDateTime.now.getOffset.getTotalSeconds)
      )
      .toInt
  }

  def pushDownType(): PushDownType = {
    if (useStreamingProcess()) {
      PushDownType.STREAMING
    } else {
      PushDownType.NORMAL
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan
      .collectFirst {
        case LogicalRelation(relation: TiDBRelation, _, _) =>
          doPlan(relation, plan)
      }
      .toSeq
      .flatten
  }

  private def toCoprocessorRDD(source: TiDBRelation,
                               output: Seq[Attribute],
                               dagRequest: TiDAGRequest): SparkPlan = {
    val table = source.table
    dagRequest.setTableInfo(table)

    if (dagRequest.getFields.isEmpty) {
      dagRequest.addRequiredColumn(ColumnRef.create(table.getColumns.get(0).getName))
    }
    dagRequest.resolve()

    val notAllowPushDown = dagRequest.getFields
      .map { _.getColumnInfo.getType.simpleTypeName }
      .exists { typeBlackList.isUnsupportedType }

    if (notAllowPushDown) {
      throw new IgnoreUnsupportedTypeException("Unsupported type found in fields: " + typeBlackList)
    } else {
      if (dagRequest.isIndexScan) {
        source.dagRequestToRegionTaskExec(dagRequest, output)
      } else {
        val tiRdd = source.logicalPlanToRDD(dagRequest)
        CoprocessorRDD(output, tiRdd)
      }
    }
  }

  def aggregationToDAGRequest(
    groupByList: Seq[NamedExpression],
    aggregates: Seq[AggregateExpression],
    source: TiDBRelation,
    dagRequest: TiDAGRequest = new TiDAGRequest(pushDownType(), timeZoneOffset())
  ): TiDAGRequest = {
    aggregates.foreach {
      case AggregateExpression(_: Average, _, _, _) =>
        throw new IllegalArgumentException("Should never be here")

      case AggregateExpression(f @ Sum(BasicExpression(arg)), _, _, _) =>
        dagRequest.addAggregate(new TiSum(arg), fromSparkType(f.dataType))

      case AggregateExpression(f @ Count(args), _, _, _) =>
        val tiArgs = args.flatMap(BasicExpression.convertToTiExpr)
        dagRequest.addAggregate(new TiCount(tiArgs: _*), fromSparkType(f.dataType))

      case AggregateExpression(f @ Min(BasicExpression(arg)), _, _, _) =>
        dagRequest.addAggregate(new TiMin(arg), fromSparkType(f.dataType))

      case AggregateExpression(f @ Max(BasicExpression(arg)), _, _, _) =>
        dagRequest.addAggregate(new TiMax(arg), fromSparkType(f.dataType))

      case AggregateExpression(f @ First(BasicExpression(arg), _), _, _, _) =>
        dagRequest.addAggregate(new TiFirst(arg), fromSparkType(f.dataType))

      case _ =>
    }

    groupByList.foreach {
      case BasicExpression(keyExpr) =>
        dagRequest.addGroupByItem(ByItem.create(keyExpr, false))

      case _ =>
    }

    dagRequest
  }

  def extractColumnFromFilter(tiFilter: expression.Expression,
                              result: ArrayBuffer[ColumnRef]): Unit =
    tiFilter match {
      case fun: TiScalarFunction =>
        fun.getArgs.foreach(extractColumnFromFilter(_, result))
      case col: ColumnRef =>
        result.add(col)
      case _ =>
    }

  def filterToDAGRequest(
    filters: Seq[Expression],
    source: TiDBRelation,
    dagRequest: TiDAGRequest = new TiDAGRequest(pushDownType(), timeZoneOffset())
  ): TiDAGRequest = {
    val tiFilters: Seq[expression.Expression] = filters.collect {
      case BasicExpression(expr) => expr
    }
    val scanBuilder: ScanBuilder = new ScanBuilder
    val tableScanPlan =
      scanBuilder.buildTableScan(JavaConversions.seqAsJavaList(tiFilters), source.table)
    val scanPlan = if (allowIndexDoubleRead()) {
      // We need to prepare downgrade information in case of index scan downgrade happens.
      tableScanPlan.getFilters.foreach(dagRequest.addDowngradeFilter)
      scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters), source.table)
    } else {
      tableScanPlan
    }

    dagRequest.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.foreach(dagRequest.addFilter)
    if (scanPlan.isIndexScan) {
      dagRequest.setIndexInfo(scanPlan.getIndex)
    }
    dagRequest
  }

  def addSortOrder(request: TiDAGRequest, sortOrder: Seq[SortOrder]): Unit =
    if (sortOrder != null) {
      sortOrder.foreach(
        (order: SortOrder) =>
          request.addOrderByItem(
            ByItem.create(
              BasicExpression.convertToTiExpr(order.child).get,
              order.direction.sql.equalsIgnoreCase("DESC")
            )
        )
      )
    }

  def pruneTopNFilterProject(
    limit: Int,
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    source: TiDBRelation,
    sortOrder: Seq[SortOrder] = null
  ): SparkPlan = {
    val request = new TiDAGRequest(pushDownType(), timeZoneOffset())
    request.setLimit(limit)
    addSortOrder(request, sortOrder)
    pruneFilterProject(projectList, filterPredicates, source, request)
  }

  def collectLimit(limit: Int, child: LogicalPlan): SparkPlan = child match {
    case PhysicalOperation(projectList, filters, LogicalRelation(source: TiDBRelation, _, _))
        if filters.forall(TiUtils.isSupportedFilter(_, source, blacklist)) =>
      pruneTopNFilterProject(limit, projectList, filters, source, null)
    case _ => planLater(child)
  }

  def takeOrderedAndProject(
    limit: Int,
    sortOrder: Seq[SortOrder],
    child: LogicalPlan,
    project: Seq[NamedExpression]
  ): SparkPlan = {
    // If sortOrder is not null, limit must be greater than 0
    if (limit < 0 || (sortOrder == null && limit == 0)) {
      return execution.TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }

    child match {
      case PhysicalOperation(projectList, filters, LogicalRelation(source: TiDBRelation, _, _))
          if filters.forall(TiUtils.isSupportedFilter(_, source, blacklist)) =>
        execution.TakeOrderedAndProjectExec(
          limit,
          sortOrder,
          project,
          pruneTopNFilterProject(limit, projectList, filters, source, sortOrder)
        )
      case _ => execution.TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }
  }

  def pruneFilterProject(
    projectList: Seq[NamedExpression],
    filterPredicates: Seq[Expression],
    source: TiDBRelation,
    dagRequest: TiDAGRequest = new TiDAGRequest(pushDownType(), timeZoneOffset())
  ): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val (pushdownFilters: Seq[Expression], residualFilters: Seq[Expression]) =
      filterPredicates.partition(
        (expression: Expression) => TiUtils.isSupportedFilter(expression, source, blacklist)
      )

    val residualFilter: Option[Expression] =
      residualFilters.reduceLeftOption(catalyst.expressions.And)

    filterToDAGRequest(pushdownFilters, source, dagRequest)

    // Right now we still use a projection even if the only evaluation is applying an alias
    // to a column.  Since this is a no-op, it could be avoided. However, using this
    // optimization with the current implementation would change the output schema.
    // TODO: Decouple final output schema from expression evaluation so this copy can be
    // avoided safely.
    if (AttributeSet(projectList.map(_.toAttribute)) == projectSet &&
        filterSet.subsetOf(projectSet)) {
      // When it is possible to just use column pruning to get the right projection and
      // when the columns of this projection are enough to evaluate all filter conditions,
      // just do a scan followed by a filter, with no extra project.
      val projectSeq: Seq[Attribute] = projectList.asInstanceOf[Seq[Attribute]]
      projectSeq.foreach(attr => dagRequest.addRequiredColumn(ColumnRef.create(attr.name)))
      val scan = toCoprocessorRDD(source, projectSeq, dagRequest)
      residualFilter.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      // for now all column used will be returned for old interface
      // TODO: once switch to new interface we change this pruning logic
      val projectSeq: Seq[Attribute] = (projectSet ++ filterSet).toSeq
      projectSeq.foreach(attr => dagRequest.addRequiredColumn(ColumnRef.create(attr.name)))
      val scan = toCoprocessorRDD(source, projectSeq, dagRequest)
      ProjectExec(projectList, residualFilter.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

  def groupAggregateProjection(
    filters: Seq[Expression],
    groupingExpressions: Seq[NamedExpression],
    aggregateExpressions: Seq[AggregateExpression],
    resultExpressions: Seq[NamedExpression],
    projects: Seq[NamedExpression],
    source: TiDBRelation,
    dagReq: TiDAGRequest
  ): Seq[SparkPlan] = {
    val aliasMap = mutable.HashMap[(Boolean, Expression), Alias]()
    val avgPushdownRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()
    val avgFinalRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()

    def newAggregate(aggFunc: AggregateFunction, originalAggExpr: AggregateExpression) =
      AggregateExpression(
        aggFunc,
        originalAggExpr.mode,
        originalAggExpr.isDistinct,
        originalAggExpr.resultId
      )

    def newAggregateWithId(aggFunc: AggregateFunction, originalAggExpr: AggregateExpression) =
      AggregateExpression(
        aggFunc,
        originalAggExpr.mode,
        originalAggExpr.isDistinct,
        NamedExpression.newExprId
      )

    def toAlias(expr: AggregateExpression) =
      if (!expr.deterministic) {
        Alias(expr, expr.toString())()
      } else {
        aliasMap.getOrElseUpdate(
          (expr.deterministic, expr.canonicalized),
          Alias(expr, expr.toString)()
        )
      }

    val residualAggregateExpressions = aggregateExpressions.map { aggExpr =>
      aggExpr.aggregateFunction match {
        // here aggExpr is the original AggregationExpression
        // and will be pushed down to TiKV
        case Max(_)   => newAggregate(Max(toAlias(aggExpr).toAttribute), aggExpr)
        case Min(_)   => newAggregate(Min(toAlias(aggExpr).toAttribute), aggExpr)
        case Count(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
        case Sum(_)   => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
        case First(_, ignoreNullsExpr) =>
          newAggregate(First(toAlias(aggExpr).toAttribute, ignoreNullsExpr), aggExpr)
        case _ => aggExpr
      }
    } flatMap { aggExpr =>
      aggExpr match {
        // We have to separate average into sum and count
        // and for outside expression such as average(x) + 1,
        // Spark has lift agg + 1 up to resultExpressions
        // We need to modify the reference there as well to forge
        // Divide(sum/count) + 1
        case aggExpr @ AggregateExpression(Average(ref), _, _, _) =>
          // Need a type promotion
          val sumToPush = newAggregate(Sum(ref), aggExpr)
          val countToPush = newAggregate(Count(ref), aggExpr)

          // Need a new expression id since they are not simply rewrite as above
          val sumFinal = newAggregateWithId(Sum(toAlias(sumToPush).toAttribute), aggExpr)
          val countFinal = newAggregateWithId(Sum(toAlias(countToPush).toAttribute), aggExpr)

          avgPushdownRewriteMap(aggExpr.resultId) = List(sumToPush, countToPush)
          avgFinalRewriteMap(aggExpr.resultId) = List(sumFinal, countFinal)
          List(sumFinal, countFinal)
        case _ => aggExpr :: Nil
      }
    }

    val pushdownAggregates = aggregateExpressions.flatMap { aggExpr =>
      avgPushdownRewriteMap
        .getOrElse(aggExpr.resultId, List(aggExpr))
    }.distinct

    aggregationToDAGRequest(groupingExpressions, pushdownAggregates, source, dagReq)

    val rewrittenResultExpression = resultExpressions.map(
      expr =>
        expr
          .transformDown {
            case aggExpr: AttributeReference if avgFinalRewriteMap.contains(aggExpr.exprId) =>
              // Replace the original Average expression with Div of Alias
              val sumCountPair = avgFinalRewriteMap(aggExpr.exprId)

              // We missed the chance for auto-coerce already
              // so manual cast needed
              // Also, convert into resultAttribute since
              // they are created by tiSpark without Spark conversion
              // TODO: Is DoubleType a best target type for all?
              Cast(
                Divide(
                  Cast(sumCountPair.head.resultAttribute, DoubleType),
                  Cast(sumCountPair(1).resultAttribute, DoubleType)
                ),
                aggExpr.dataType
              )
            case other => other
          }
          .asInstanceOf[NamedExpression]
    )

    val output = (pushdownAggregates.map(x => toAlias(x)) ++ groupingExpressions)
      .map(_.toAttribute)

    val projectSeq: Seq[Attribute] = projects.asInstanceOf[Seq[Attribute]]
    projectSeq.foreach(attr => dagReq.addRequiredColumn(ColumnRef.create(attr.name)))
    val pushDownCols = ArrayBuffer[ColumnRef]()
    val tiFilters: Seq[expression.Expression] = filters.collect {
      case BasicExpression(expr) => expr
    }
    tiFilters.foreach(extractColumnFromFilter(_, pushDownCols))
    pushDownCols.foreach(dagReq.addRequiredColumn)

    aggregate.AggUtils.planAggregateWithoutDistinct(
      groupingExpressions,
      residualAggregateExpressions,
      rewrittenResultExpression,
      toCoprocessorRDD(source, output, dagReq)
    )
  }

  def isValidAggregates(groupingExpressions: Seq[NamedExpression],
                        aggregateExpressions: Seq[AggregateExpression],
                        filters: Seq[Expression],
                        source: TiDBRelation): Boolean = {
    allowAggregationPushDown &&
    filters.forall(TiUtils.isSupportedFilter(_, source, blacklist)) &&
    groupingExpressions.forall(TiUtils.isSupportedGroupingExpr(_, source, blacklist)) &&
    aggregateExpressions.forall(TiUtils.isSupportedAggregate(_, source, blacklist)) &&
    !aggregateExpressions.exists(_.isDistinct)
  }

  // We do through similar logic with original Spark as in SparkStrategies.scala
  // Difference is we need to test if a sub-plan can be consumed all together by TiKV
  // and then we don't return (don't planLater) and plan the remaining all at once
  private def doPlan(source: TiDBRelation, plan: LogicalPlan): Seq[SparkPlan] = {
    // TODO: This test should be done once for all children
    plan match {
      case logical.ReturnAnswer(rootPlan) =>
        rootPlan match {
          case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
            takeOrderedAndProject(limit, order, child, child.output) :: Nil
          case logical.Limit(
              IntegerLiteral(limit),
              logical.Project(projectList, logical.Sort(order, true, child))
              ) =>
            takeOrderedAndProject(limit, order, child, projectList) :: Nil
          case logical.Limit(IntegerLiteral(limit), child) =>
            execution.CollectLimitExec(limit, collectLimit(limit, child)) :: Nil
          case other => planLater(other) :: Nil
        }
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
        takeOrderedAndProject(limit, order, child, child.output) :: Nil
      case logical.Limit(
          IntegerLiteral(limit),
          logical.Project(projectList, logical.Sort(order, true, child))
          ) =>
        takeOrderedAndProject(limit, order, child, projectList) :: Nil

      // Collapse filters and projections and push plan directly
      case PhysicalOperation(projectList, filters, LogicalRelation(source: TiDBRelation, _, _)) =>
        pruneFilterProject(projectList, filters, source) :: Nil

      // Basic logic of original Spark's aggregation plan is:
      // PhysicalAggregation extractor will rewrite original aggregation
      // into aggregateExpressions and resultExpressions.
      // resultExpressions contains only references [[AttributeReference]]
      // to the result of aggregation. resultExpressions might contain projections
      // like Add(sumResult, 1).
      // For a aggregate like agg(expr) + 1, the rewrite process is: rewrite agg(expr) ->
      // 1. pushdown: agg(expr) as agg1, if avg then sum(expr), count(expr)
      // 2. residual expr (for Spark itself): agg(agg1) as finalAgg1 the parameter is a
      // reference to pushed plan's corresponding aggregation
      // 3. resultExpressions: finalAgg1 + 1, the finalAgg1 is the reference to final result
      // of the aggregation
      case TiAggregation(
          groupingExpressions,
          aggregateExpressions,
          resultExpressions,
          TiAggregationProjection(filters, _, `source`, projects)
          ) if isValidAggregates(groupingExpressions, aggregateExpressions, filters, source) =>
        val dagReq: TiDAGRequest = filterToDAGRequest(filters, source)
        groupAggregateProjection(
          filters,
          groupingExpressions,
          aggregateExpressions,
          resultExpressions,
          projects,
          `source`,
          dagReq
        )
      case _ => Nil
    }
  }
}

object TiAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def unapply(a: Any): Option[ReturnType] = a match {
    case PhysicalAggregation(groupingExpressions, aggregateExpressions, resultExpressions, child) =>
      Some(groupingExpressions, aggregateExpressions, resultExpressions, child)

    case _ => Option.empty[ReturnType]
  }
}

object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBRelation, Seq[NamedExpression])

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    // Only push down aggregates projection when all filters can be applied and
    // all projection expressions are column references
    case PhysicalOperation(projects, filters, rel @ LogicalRelation(source: TiDBRelation, _, _))
        if projects.forall(_.isInstanceOf[Attribute]) =>
      Some((filters, rel, source, projects))
    case _ => Option.empty[ReturnType]
  }
}
