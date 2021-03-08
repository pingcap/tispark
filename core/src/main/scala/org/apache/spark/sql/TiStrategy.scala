/*
 * Copyright 2019 PingCAP, Inc.
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

import java.util.concurrent.TimeUnit

import com.pingcap.tidb.tipb.EncodeType
import com.pingcap.tikv.exception.IgnoreUnsupportedTypeException
import com.pingcap.tikv.expression._
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType
import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.predicates.{PredicateUtils, TiKVScanAnalyzer}
import com.pingcap.tikv.region.TiStoreType
import com.pingcap.tikv.statistics.TableStatistics
import com.pingcap.tispark.statistics.StatisticsManager
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.{TiConfigConst, TiDBRelation}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{
  Alias,
  Ascending,
  Attribute,
  AttributeMap,
  AttributeSet,
  Descending,
  TiExprUtils,
  Expression,
  IntegerLiteral,
  IsNull,
  NamedExpression,
  NullsFirst,
  NullsLast,
  SortOrder,
  SubqueryExpression
}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{ColumnarCoprocessorRDD, _}
import org.apache.spark.sql.internal.SQLConf
import org.joda.time.{DateTime, DateTimeZone}

import scala.collection.JavaConverters._
import scala.collection.mutable

object TiStrategy {
  private val assignedTSPlanCache = new mutable.WeakHashMap[LogicalPlan, Boolean]()

  private def hasTSAssigned(plan: LogicalPlan): Boolean = {
    assignedTSPlanCache.contains(plan)
  }

  private def markTSAssigned(plan: LogicalPlan): Unit = {
    plan foreachUp { p =>
      assignedTSPlanCache.put(p, true)
    }
  }
}

/**
 * CHECK Spark [[org.apache.spark.sql.Strategy]]
 *
 * TODO: Too many hacks here since we hijack the planning
 * but we don't have full control over planning stage
 * We cannot pass context around during planning so
 * a re-extract needed for push-down since
 * a plan tree might contain Join which causes a single tree
 * have multiple plans to push-down
 */
case class TiStrategy(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession)
    extends Strategy
    with Logging {
  type TiExpression = com.pingcap.tikv.expression.Expression
  type TiColumnRef = com.pingcap.tikv.expression.ColumnRef
  private lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val sqlContext = tiContext.sqlContext
  private lazy val sqlConf: SQLConf = sqlContext.conf

  def typeBlockList: TypeBlocklist = {
    val blocklistString =
      sqlConf.getConfString(TiConfigConst.UNSUPPORTED_TYPES, "")
    new TypeBlocklist(blocklistString)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val ts = tiContext.tiSession.getTimestamp

    if (plan.isStreaming) {
      // We should use a new timestamp for next batch execution.
      // Otherwise Spark Structure Streaming will not see new data in TiDB.
      if (!TiStrategy.hasTSAssigned(plan)) {
        plan foreachUp applyStartTs(ts, forceUpdate = true)
        TiStrategy.markTSAssigned(plan)
      }
    } else {
      plan foreachUp applyStartTs(ts)
    }

    plan
      .collectFirst {
        case LogicalRelation(relation: TiDBRelation, _, _, _) =>
          doPlan(relation, plan)
      }
      .toSeq
      .flatten
  }

  def referencedTiColumns(expression: TiExpression): Seq[TiColumnRef] =
    PredicateUtils.extractColumnRefFromExpression(expression).asScala.toSeq

  /**
   * build a Seq of used TiColumnRef from AttributeSet and bound them to source table
   *
   * @param attributeSet AttributeSet containing projects w/ or w/o filters
   * @param source       source TiDBRelation
   * @return a Seq of TiColumnRef extracted
   */
  def buildTiColumnRefFromColumnSeq(
      attributeSet: AttributeSet,
      source: TiDBRelation): Seq[TiColumnRef] = {
    val tiColumnSeq: Seq[TiExpression] = attributeSet.toSeq.map { expr =>
      TiExprUtils.transformAttrToColRef(expr, source.table)
    }
    var tiColumns: mutable.HashSet[TiColumnRef] = mutable.HashSet.empty[TiColumnRef]
    for (expression <- tiColumnSeq) {
      val colSetPerExpr = PredicateUtils.extractColumnRefFromExpression(expression)
      colSetPerExpr.asScala.foreach {
        tiColumns += _
      }
    }
    tiColumns.toSeq
  }

  // apply StartTs to every logical plan in Spark Planning stage
  protected def applyStartTs(
      ts: TiTimestamp,
      forceUpdate: Boolean = false): PartialFunction[LogicalPlan, Unit] = {
    case LogicalRelation(r @ TiDBRelation(_, _, _, timestamp, _), _, _, _) =>
      if (timestamp == null || forceUpdate) {
        r.ts = ts
      }
    case logicalPlan =>
      logicalPlan transformExpressionsUp {
        case s: SubqueryExpression =>
          s.plan.foreachUp(applyStartTs(ts))
          s
      }
  }

  private def blocklist: ExpressionBlocklist = {
    val blocklistString = sqlConf.getConfString(TiConfigConst.UNSUPPORTED_PUSHDOWN_EXPR, "")
    new ExpressionBlocklist(blocklistString)
  }

  private def allowAggregationPushDown(): Boolean =
    sqlConf.getConfString(TiConfigConst.ALLOW_AGG_PUSHDOWN, "true").toLowerCase.toBoolean

  private def useIndexScanFirst(): Boolean =
    sqlConf.getConfString(TiConfigConst.USE_INDEX_SCAN_FIRST, "false").toLowerCase.toBoolean

  private def allowIndexRead(): Boolean =
    sqlConf.getConfString(TiConfigConst.ALLOW_INDEX_READ, "true").toLowerCase.toBoolean

  private def useStreamingProcess: Boolean =
    sqlConf.getConfString(TiConfigConst.COPROCESS_STREAMING, "false").toLowerCase.toBoolean

  private def getCodecFormat: EncodeType = {
    // FIXME: Should use default codec format "chblock", change it back after fix.
    val codecFormatStr =
      sqlConf
        .getConfString(TiConfigConst.CODEC_FORMAT, TiConfigConst.DEFAULT_CODEC_FORMAT)
        .toLowerCase

    codecFormatStr match {
      case TiConfigConst.CHUNK_CODEC_FORMAT => EncodeType.TypeChunk
      case TiConfigConst.DEFAULT_CODEC_FORMAT => EncodeType.TypeCHBlock
      case _ => EncodeType.TypeDefault
    }
  }

  private def eligibleStorageEngines(source: TiDBRelation): List[TiStoreType] =
    TiUtil.getIsolationReadEngines(sqlContext).filter {
      case TiStoreType.TiKV => true
      case TiStoreType.TiFlash => source.isTiFlashReplicaAvailable
      case _ => false
    }

  private def timeZoneOffsetInSeconds(): Int = {
    val tz = DateTimeZone.getDefault
    val instant = DateTime.now.getMillis
    val offsetInMilliseconds = tz.getOffset(instant)
    val hours = TimeUnit.MILLISECONDS.toHours(offsetInMilliseconds).toInt
    val seconds = hours * 3600
    seconds
  }

  private def newTiDAGRequest(): TiDAGRequest = {
    val ts = timeZoneOffsetInSeconds()
    if (useStreamingProcess) {
      new TiDAGRequest(PushDownType.STREAMING, ts)
    } else {
      new TiDAGRequest(PushDownType.NORMAL, getCodecFormat, ts)
    }
  }

  private def toCoprocessorRDD(
      source: TiDBRelation,
      output: Seq[Attribute],
      dagRequest: TiDAGRequest): SparkPlan = {
    dagRequest.setTableInfo(source.table)
    dagRequest.setStartTs(source.ts)

    val notAllowPushDown = dagRequest.getFields.asScala
      .map {
        _.getDataType.getType
      }
      .exists {
        typeBlockList.isUnsupportedType
      }

    if (notAllowPushDown) {
      throw new IgnoreUnsupportedTypeException(
        "Unsupported type found in fields: " + typeBlockList)
    } else {
      if (dagRequest.isDoubleRead) {
        source.dagRequestToRegionTaskExec(dagRequest, output)
      } else {
        ColumnarCoprocessorRDD(
          output,
          source.logicalPlanToRDD(dagRequest, output),
          fetchHandle = false)
      }
    }
  }

  private def aggregationToDAGRequest(
      groupByList: Seq[NamedExpression],
      aggregates: Seq[AggregateExpression],
      source: TiDBRelation,
      dagRequest: TiDAGRequest): TiDAGRequest = {
    aggregates
      .map {
        _.aggregateFunction
      }
      .foreach { expr =>
        TiExprUtils.transformAggExprToTiAgg(expr, source.table, dagRequest)
      }

    groupByList.foreach { expr =>
      TiExprUtils.transformGroupingToTiGrouping(expr, source.table, dagRequest)
    }

    dagRequest
  }

  private def filterToDAGRequest(
      tiColumns: Seq[TiColumnRef],
      filters: Seq[Expression],
      source: TiDBRelation,
      dagRequest: TiDAGRequest): TiDAGRequest = {
    val tiFilters: Seq[TiExpression] = filters.map {
      TiExprUtils.transformFilter(_, source.table, dagRequest)
    }

    val scanBuilder: TiKVScanAnalyzer = new TiKVScanAnalyzer

    val tblStatistics: TableStatistics = StatisticsManager.getTableStatistics(source.table.getId)

    // engines that could be chosen.
    val engines = eligibleStorageEngines(source)

    if (engines.isEmpty) {
      throw new RuntimeException(
        s"No eligible storage engines found for $source, " +
          s"isolation_read_engines = ${TiUtil.getIsolationReadEngines(sqlContext)}")
    }

    scanBuilder.buildTiDAGReq(
      allowIndexRead(),
      useIndexScanFirst(),
      engines.contains(TiStoreType.TiKV),
      engines.contains(TiStoreType.TiFlash),
      tiColumns.map { colRef =>
        source.table.getColumn(colRef.getName)
      }.asJava,
      tiFilters.asJava,
      source.table,
      tblStatistics,
      source.ts,
      dagRequest)
  }

  private def pruneTopNFilterProject(
      limit: Int,
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      source: TiDBRelation,
      sortOrder: Seq[SortOrder]): SparkPlan = {
    val request = newTiDAGRequest()
    request.setLimit(limit)
    TiExprUtils.transformSortOrderToTiOrderBy(request, sortOrder, source.table)

    pruneFilterProject(projectList, filterPredicates, source, request)
  }

  private def collectLimit(limit: Int, child: LogicalPlan): SparkPlan =
    child match {
      case PhysicalOperation(projectList, filters, LogicalRelation(source: TiDBRelation, _, _, _))
          if filters.forall(TiExprUtils.isSupportedFilter(_, source, blocklist)) =>
        pruneTopNFilterProject(limit, projectList, filters, source, Nil)
      case _ => planLater(child)
    }

  private def takeOrderedAndProject(
      limit: Int,
      sortOrder: Seq[SortOrder],
      child: LogicalPlan,
      project: Seq[NamedExpression]): SparkPlan = {
    // If sortOrder is empty, limit must be greater than 0
    if (limit < 0 || (sortOrder.isEmpty && limit == 0)) {
      return execution.TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }

    child match {
      case PhysicalOperation(projectList, filters, LogicalRelation(source: TiDBRelation, _, _, _))
          if filters.forall(TiExprUtils.isSupportedFilter(_, source, blocklist)) =>
        val refinedOrders = refineSortOrder(projectList, sortOrder, source)
        if (refinedOrders.isEmpty) {
          execution.TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
        } else {
          execution.TakeOrderedAndProjectExec(
            limit,
            sortOrder,
            project,
            pruneTopNFilterProject(limit, projectList, filters, source, refinedOrders.get))
        }
      case _ => execution.TakeOrderedAndProjectExec(limit, sortOrder, project, planLater(child))
    }
  }

  // refine sort order
  // 1. sort order expressions are all valid to be pushed
  // 2. if any reference to projections are valid to be pushed
  private def refineSortOrder(
      projectList: Seq[NamedExpression],
      sortOrders: Seq[SortOrder],
      source: TiDBRelation): Option[Seq[SortOrder]] = {
    val aliases = AttributeMap(projectList.collect {
      case a: Alias => a.toAttribute -> a
    })
    // Order by desc/asc + nulls first/last
    //
    // 1. Order by asc + nulls first:
    //	  order by col asc nulls first = order by col asc
    //  2. Order by desc + nulls first:
    // 	  order by col desc nulls first = order by col is null desc, col desc
    //  3. Order by asc + nulls last:
    //	  order by col asc nulls last = order by col is null asc, col asc
    //  4. Order by desc + nulls last:
    //	  order by col desc nulls last = order by col desc
    val refinedSortOrder = sortOrders.flatMap { sortOrder: SortOrder =>
      val newSortExpr = sortOrder.child.transformUp {
        case a: Attribute => aliases.getOrElse(a, a)
      }
      val trimmedExpr = CleanupAliases.trimNonTopLevelAliases(newSortExpr)
      val trimmedSortOrder = sortOrder.copy(child = trimmedExpr)
      (sortOrder.direction, sortOrder.nullOrdering) match {
        case (_ @Ascending, _ @NullsLast) | (_ @Descending, _ @NullsFirst) =>
          sortOrder.copy(child = IsNull(trimmedExpr)) :: trimmedSortOrder :: Nil
        case _ =>
          trimmedSortOrder :: Nil
      }
    }
    if (refinedSortOrder
        .exists(order => !TiExprUtils.isSupportedOrderBy(order.child, source, blocklist))) {
      Option.empty
    } else {
      Some(refinedSortOrder)
    }
  }

  private def pruneFilterProject(
      projectList: Seq[NamedExpression],
      filterPredicates: Seq[Expression],
      source: TiDBRelation,
      dagRequest: TiDAGRequest): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val (pushdownFilters: Seq[Expression], residualFilters: Seq[Expression]) =
      filterPredicates.partition((expression: Expression) =>
        TiExprUtils.isSupportedFilter(expression, source, blocklist))

    val residualFilter: Option[Expression] =
      residualFilters.reduceLeftOption(catalyst.expressions.And)

    val tiColumns = buildTiColumnRefFromColumnSeq(projectSet ++ filterSet, source)

    filterToDAGRequest(tiColumns, pushdownFilters, source, dagRequest)

    if (tiColumns.isEmpty) {
      // we cannot send a request with empty columns
      if (dagRequest.hasIndex) {
        // add the first index column so that the plan will contain at least one column.
        val idxColumn = dagRequest.getIndexInfo.getIndexColumns.get(0)
        dagRequest.addRequiredColumn(ColumnRef.create(idxColumn.getName, source.table))
      } else {
        // add a random column so that the plan will contain at least one column.
        // if the table contains a primary key then use the PK instead.
        val column = source.table.getColumns.asScala
          .collectFirst {
            case e if e.isPrimaryKey => e
          }
          .getOrElse(source.table.getColumn(0))
        dagRequest.addRequiredColumn(ColumnRef.create(column.getName, source.table))
      }
    }

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
      projectSeq.foreach(
        attr =>
          dagRequest.addRequiredColumn(
            ColumnRef.create(attr.name, source.table.getColumn(attr.name))))
      val scan = toCoprocessorRDD(source, projectSeq, dagRequest)
      residualFilter.fold(scan)(FilterExec(_, scan))
    } else {
      // for now all column used will be returned for old interface
      // TODO: once switch to new interface we change this pruning logic
      val projectSeq: Seq[Attribute] = (projectSet ++ filterSet).toSeq
      projectSeq.foreach(
        attr =>
          dagRequest.addRequiredColumn(
            ColumnRef.create(attr.name, source.table.getColumn(attr.name))))
      val scan = toCoprocessorRDD(source, projectSeq, dagRequest)
      ProjectExec(projectList, residualFilter.fold(scan)(FilterExec(_, scan)))
    }
  }

  private def groupAggregateProjection(
      tiColumns: Seq[TiColumnRef],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      source: TiDBRelation,
      dagReq: TiDAGRequest): Seq[SparkPlan] = {
    val deterministicAggAliases = aggregateExpressions.collect {
      case e if e.deterministic => e.canonicalized -> Alias(e, e.toString())()
    }.toMap

    def aliasPushedPartialResult(e: AggregateExpression): Alias =
      deterministicAggAliases.getOrElse(e.canonicalized, Alias(e, e.toString())())

    val residualAggregateExpressions = aggregateExpressions.map { aggExpr =>
      // As `aggExpr` is being pushing down to TiKV, we need to replace the original Catalyst
      // aggregate expressions with new ones that merges the partial aggregation results returned by
      // TiKV.
      //
      // NOTE: Unlike simple aggregate functions (e.g., `Max`, `Min`, etc.), `Count` must be
      // replaced with a `Sum` to sum up the partial counts returned by TiKV.
      //
      // NOTE: All `Average`s should have already been rewritten into `Sum`s and `Count`s by the
      // `TiAggregation` pattern extractor.

      // An attribute referring to the partial aggregation results returned by TiKV.
      val partialResultRef = aliasPushedPartialResult(aggExpr).toAttribute

      aggExpr.aggregateFunction match {
        case e: Max => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Min => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: Sum => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: SpecialSum => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case e: First => aggExpr.copy(aggregateFunction = e.copy(child = partialResultRef))
        case _: Count =>
          aggExpr.copy(aggregateFunction = SumNotNullable(partialResultRef))
        case _: Average => throw new IllegalStateException("All AVGs should have been rewritten.")
        case _ => aggExpr
      }
    }

    tiColumns foreach {
      dagReq.addRequiredColumn
    }

    aggregationToDAGRequest(groupingExpressions, aggregateExpressions.distinct, source, dagReq)

    val aggregateAttributes =
      aggregateExpressions.map(expr => aliasPushedPartialResult(expr).toAttribute)
    val groupAttributes = groupingExpressions.map(_.toAttribute)

    // output of Coprocessor plan should contain all references within
    // aggregates and group by expressions
    val output = aggregateAttributes ++ groupAttributes

    val groupExpressionMap =
      groupingExpressions.map(expr => expr.exprId -> expr.toAttribute).toMap

    // resultExpression might refer to some of the group by expressions
    // Those expressions originally refer to table columns but now it refers to
    // results of coprocessor.
    // For example, select a + 1 from t group by a + 1
    // expression a + 1 has been pushed down to coprocessor
    // and in turn a + 1 in projection should be replaced by
    // reference of coprocessor output entirely
    val rewrittenResultExpressions = resultExpressions.map {
      _.transform {
        case e: NamedExpression => groupExpressionMap.getOrElse(e.exprId, e)
      }.asInstanceOf[NamedExpression]
    }

    aggregate.AggUtils.planAggregateWithoutDistinct(
      groupAttributes,
      residualAggregateExpressions,
      rewrittenResultExpressions,
      toCoprocessorRDD(source, output, dagReq))
  }

  private def isValidAggregates(
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      filters: Seq[Expression],
      source: TiDBRelation): Boolean =
    allowAggregationPushDown &&
      filters.forall(TiExprUtils.isSupportedFilter(_, source, blocklist)) &&
      groupingExpressions.forall(TiExprUtils.isSupportedGroupingExpr(_, source, blocklist)) &&
      aggregateExpressions.forall(TiExprUtils.isSupportedAggregate(_, source, blocklist)) &&
      !aggregateExpressions.exists(_.isDistinct) &&
      // TODO: This is a temporary fix for the issue: https://github.com/pingcap/tispark/issues/1039
      !groupingExpressions.exists(_.isInstanceOf[Alias])

  // We do through similar logic with original Spark as in SparkStrategies.scala
  // Difference is we need to test if a sub-plan can be consumed all together by TiKV
  // and then we don't return (don't planLater) and plan the remaining all at once
  // TODO: This test should be done once for all children
  private def doPlan(source: TiDBRelation, plan: LogicalPlan): Seq[SparkPlan] =
    plan match {
      case logical.ReturnAnswer(rootPlan) =>
        rootPlan match {
          case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
            takeOrderedAndProject(limit, order, child, child.output) :: Nil
          case logical.Limit(
                IntegerLiteral(limit),
                logical.Project(projectList, logical.Sort(order, true, child))) =>
            takeOrderedAndProject(limit, order, child, projectList) :: Nil
          case logical.Limit(IntegerLiteral(limit), child) =>
            execution.CollectLimitExec(limit, collectLimit(limit, child)) :: Nil
          case other => planLater(other) :: Nil
        }
      case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
        takeOrderedAndProject(limit, order, child, child.output) :: Nil
      case logical.Limit(
            IntegerLiteral(limit),
            logical.Project(projectList, logical.Sort(order, true, child))) =>
        takeOrderedAndProject(limit, order, child, projectList) :: Nil
      // Collapse filters and projections and push plan directly
      case PhysicalOperation(
            projectList,
            filters,
            LogicalRelation(source: TiDBRelation, _, _, _)) =>
        pruneFilterProject(projectList, filters, source, newTiDAGRequest()) :: Nil

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
            TiAggregationProjection(filters, _, `source`, projects))
          if isValidAggregates(groupingExpressions, aggregateExpressions, filters, source) =>
        val projectSet = AttributeSet((projects ++ filters).flatMap {
          _.references
        })
        val tiColumns = buildTiColumnRefFromColumnSeq(projectSet, source)
        val dagReq: TiDAGRequest =
          filterToDAGRequest(tiColumns, filters, source, newTiDAGRequest())
        groupAggregateProjection(
          tiColumns,
          groupingExpressions,
          aggregateExpressions,
          resultExpressions,
          `source`,
          dagReq)
      case _ => Nil
    }
}
