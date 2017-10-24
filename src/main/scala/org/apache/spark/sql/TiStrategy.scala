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

import com.pingcap.tikv.expression.{TiByItem, TiColumnRef, TiExpr}
import com.pingcap.tikv.meta.{TiIndexInfo, TiSelectRequest}
import com.pingcap.tikv.predicates.ScanBuilder
import com.pingcap.tispark.TiUtils._
import com.pingcap.tispark.{BasicExpression, TiConfigConst, TiDBRelation, TiUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Cast, Divide, ExprId, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

import scala.collection.JavaConversions._
import scala.collection.{JavaConversions, mutable}


// TODO: Too many hacks here since we hijack the planning
// but we don't have full control over planning stage
// We cannot pass context around during planning so
// a re-extract needed for pushdown since
// a plan tree might have Join which causes a single tree
// have multiple plan to pushdown
class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf: SQLConf = context.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan.collectFirst {
      case LogicalRelation(relation: TiDBRelation, _, _) =>
        doPlan(relation, plan)
    }.toSeq.flatten
  }

  private def toCoprocessorRDD(source: TiDBRelation,
                               output: Seq[Attribute],
                               selectRequest: TiSelectRequest): SparkPlan = {
    val table = source.table
    selectRequest.setTableInfo(table)
    // In case count(*) and nothing pushed, pick the first column
    if (selectRequest.getFields.size == 0) {
      selectRequest.addRequiredColumn(TiColumnRef.create(table.getColumns.get(0).getName))
    }
    val tiRdd = source.logicalPlanToRDD(selectRequest)

    CoprocessorRDD(output, tiRdd)
  }

  def aggregationToSelectRequest(groupByList: Seq[NamedExpression],
                                 aggregates: Seq[AggregateExpression],
                                 source: TiDBRelation,
                                 selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    aggregates.foreach {
      case AggregateExpression(_: Average, _, _, _) =>
        throw new IllegalArgumentException("Should never be here")

      case AggregateExpression(f @ Sum(BasicExpression(arg)), _, _, _) =>
        selectRequest.addAggregate(new TiSum(arg), fromSparkType(f.dataType))

      case AggregateExpression(f @ Count(args), _, _, _) =>
        val tiArgs = args.flatMap(BasicExpression.convertToTiExpr)
        selectRequest.addAggregate(new TiCount(tiArgs: _*), fromSparkType(f.dataType))

      case AggregateExpression(f @ Min(BasicExpression(arg)), _, _, _) =>
        selectRequest.addAggregate(new TiMin(arg), fromSparkType(f.dataType))

      case AggregateExpression(f @ Max(BasicExpression(arg)), _, _, _) =>
        selectRequest.addAggregate(new TiMax(arg), fromSparkType(f.dataType))

      case AggregateExpression(f @ First(BasicExpression(arg), _), _, _, _) =>
        selectRequest.addAggregate(new TiFirst(arg), fromSparkType(f.dataType))

      case _ =>
    }

    groupByList.foreach {
      case BasicExpression(keyExpr) =>
        selectRequest.addGroupByItem(TiByItem.create(keyExpr, false))

      case _ =>
    }

    selectRequest
  }

  def filterToSelectRequest(filters: Seq[Expression],
                            source: TiDBRelation,
                            selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    val tiFilters:Seq[TiExpr] = filters.collect { case BasicExpression(expr) => expr }
    val scanBuilder: ScanBuilder = new ScanBuilder
    val pkIndex: TiIndexInfo = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
    val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
                                         pkIndex, source.table)

    selectRequest.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.toList.map(selectRequest.addWhere)
    selectRequest
  }

  def pruneFilterProject(projectList: Seq[NamedExpression],
                         filterPredicates: Seq[Expression],
                         source: TiDBRelation,
                         selectRequest: TiSelectRequest = new TiSelectRequest): SparkPlan = {

    val projectSet = AttributeSet(projectList.flatMap(_.references))
    val filterSet = AttributeSet(filterPredicates.flatMap(_.references))

    val (pushdownFilters: Seq[Expression],
         residualFilters: Seq[Expression]) =
      filterPredicates.partition(TiUtils.isSupportedFilter)

    val residualFilter: Option[Expression] = residualFilters.reduceLeftOption(catalyst.expressions.And)

    filterToSelectRequest(pushdownFilters, source, selectRequest)

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
      projectSeq.foreach(attr => selectRequest.addRequiredColumn(TiColumnRef.create(attr.name)))
      val scan = toCoprocessorRDD(source, projectSeq, selectRequest)
      residualFilter.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      // for now all column used will be returned for old interface
      // TODO: once switch to new interface we change this pruning logic
      val projectSeq: Seq[Attribute] = (projectSet ++ filterSet).toSeq
      projectSeq.foreach(attr => selectRequest.addRequiredColumn(TiColumnRef.create(attr.name)))
      val scan = toCoprocessorRDD(source, projectSeq, selectRequest)
      ProjectExec(projectList, residualFilter.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

  // We do through similar logic with original Spark as in SparkStrategies.scala
  // Difference is we need to test if a sub-plan can be consumed all together by TiKV
  // and then we don't return (don't planLater) and plan the remaining all at once
  private def doPlan(source: TiDBRelation, plan: LogicalPlan): Seq[SparkPlan] = {

    val aliasMap = mutable.HashMap[Expression, Alias]()
    val avgPushdownRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()
    val avgFinalRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()

    def toAlias(expr: Expression) = aliasMap.getOrElseUpdate(expr, Alias(expr, expr.toString)())

    def newAggregate(aggFunc: AggregateFunction,
                     originalAggExpr: AggregateExpression) =
      AggregateExpression(aggFunc,
                          originalAggExpr.mode,
                          originalAggExpr.isDistinct,
                          originalAggExpr.resultId)

    def newAggregateWithId(aggFunc: AggregateFunction,
                           originalAggExpr: AggregateExpression) =
      AggregateExpression(aggFunc,
        originalAggExpr.mode,
        originalAggExpr.isDistinct,
        NamedExpression.newExprId)

    def allowAggregationPushdown(): Boolean = {
      sqlConf.getConfString(TiConfigConst.ALLOW_AGG_PUSHDOWN, "true").toBoolean
    }

    // TODO: This test should be done once for all children
    plan match {
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
      TiAggregationProjection(filters, _, source))
        if allowAggregationPushdown && !aggregateExpressions.exists(_.isDistinct) =>
        var selReq: TiSelectRequest = filterToSelectRequest(filters, source)
        val residualAggregateExpressions = aggregateExpressions.map {
          aggExpr =>
            aggExpr.aggregateFunction match {
              // here aggExpr is the original AggregationExpression
              // and will be pushed down to TiKV
              case Max(_) => newAggregate(Max(toAlias(aggExpr).toAttribute), aggExpr)
              case Min(_) => newAggregate(Min(toAlias(aggExpr).toAttribute), aggExpr)
              case Count(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
              case Sum(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
              case First(_, ignoreNullsExpr) =>
                newAggregate(First(toAlias(aggExpr).toAttribute, ignoreNullsExpr), aggExpr)
              case _ => aggExpr
            }
        } flatMap {
          aggExpr =>
            aggExpr match {
              // We have to separate average into sum and count
              // and for outside expression such as average(x) + 1,
              // Spark has lift agg + 1 up to resultExpressions
              // We need to modify the reference there as well to forge
              // Divide(sum/count) + 1
              case aggExpr@AggregateExpression(Average(ref), _, _, _) =>
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

        val pushdownAggregates = aggregateExpressions.flatMap {
          aggExpr =>
            avgPushdownRewriteMap
              .getOrElse(aggExpr.resultId, List(aggExpr))
        }

        selReq = aggregationToSelectRequest(groupingExpressions,
          pushdownAggregates,
          source,
          selReq)

        val rewrittenResultExpression = resultExpressions.map(
          expr => expr.transformDown {
            case aggExpr: AttributeReference
              if avgFinalRewriteMap.contains(aggExpr.exprId) =>
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
          }.asInstanceOf[NamedExpression]
        )

        val output = (groupingExpressions ++ pushdownAggregates.map(x => toAlias(x))).map(_.toAttribute)

        aggregate.AggUtils.planAggregateWithoutDistinct(
          groupingExpressions,
          residualAggregateExpressions,
          rewrittenResultExpression,
          toCoprocessorRDD(source, output, selReq))

      case _ => Nil
    }
  }
}

object TiAggregation {
  type ReturnType = PhysicalAggregation.ReturnType

  def unapply(a: Any): Option[ReturnType] = a match {
    case PhysicalAggregation(groupingExpressions, aggregateExpressions, resultExpressions, child)
      if groupingExpressions.forall(TiUtils.isSupportedGroupingExpr) &&
        aggregateExpressions.forall(TiUtils.isSupportedAggregate) =>
      Some(groupingExpressions, aggregateExpressions, resultExpressions, child)

    case _ => Option.empty[ReturnType]
  }
}

object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBRelation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = plan match {
    // Only push down aggregates projection when all filters can be applied and
    // all projection expressions are column references
    case PhysicalOperation(projects, filters, rel@LogicalRelation(source: TiDBRelation, _, _))
      if projects.forall(_.isInstanceOf[Attribute]) &&
        filters.forall(TiUtils.isSupportedFilter) =>
      Some((filters, rel, source))
    case _ => Option.empty[ReturnType]
  }
}
