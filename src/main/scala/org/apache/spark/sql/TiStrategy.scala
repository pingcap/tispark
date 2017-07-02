package org.apache.spark.sql


import com.pingcap.tikv.expression.{TiByItem, TiExpr}

import scala.collection.JavaConversions._
import com.pingcap.tikv.meta.{TiIndexInfo, TiSelectRequest}
import com.pingcap.tikv.predicates.ScanBuilder
import com.pingcap.tispark.{BasicExpression, TiDBRelation, TiUtils}
import com.pingcap.tispark.TiUtils._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, AttributeSet, Cast, Divide, ExprId, Expression, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.CatalystSource
import org.apache.spark.sql.types._

import scala.collection.{JavaConversions, mutable}


// TODO: Too many hacks here since we hijacks the planning
// but we don't have full control over planning stage
// We cannot pass context around during planning so
// a re-extract needed for pushdown since
// a plan tree might have Join which causes a single tree
// have multiple plan to pushdown
class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf = context.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val relations = plan.collect({ case p => p })
      .filter(_.isInstanceOf[LogicalRelation])
      .map(_.asInstanceOf[LogicalRelation])
      .map(_.relation)
      .toList

    if (relations.isEmpty || relations.exists(!_.isInstanceOf[TiDBRelation])) {
      Nil
    } else {
      val sources = relations.map(_.asInstanceOf[TiDBRelation])
      val source = sources.head
      doPlan(source, plan)
    }
  }

  private def toPhysicalRDD(cs: CatalystSource, plan: LogicalPlan): SparkPlan = {
    val rdd = cs.logicalPlanToRDD(plan)
    val internalRdd = RDDConversions.rowToRowRdd(rdd,
      plan.schema.fields.map(_.dataType))

    RDDScanExec(plan.output, internalRdd, "CatalystSource")
  }

  private def toCoprocessorRDD(source: TiDBRelation,
                               output: Seq[Attribute],
                               selectRequest: TiSelectRequest): SparkPlan = {
    val schemaTypes = StructType.fromAttributes(output).fields.map(_.dataType)
    selectRequest.setTableInfo(source.table)
    val rdd = source.logicalPlanToRDD(selectRequest)
    val internalRdd = RDDConversions.rowToRowRdd(rdd, schemaTypes)

    RDDScanExec(output, internalRdd, "CoprocessorRDD")
  }

  def projectFilterToSelectRequest(projects: Seq[NamedExpression],
                                   filters: Seq[Expression],
                                   source: TiDBRelation,
                                   selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    val tiFilters = filters.map(expr => expr match { case BasicExpression(expr) => expr })
    val scanBuilder = new ScanBuilder
    val pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
    val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
      pkIndex, source.table)

    selectRequest.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.toList.map(selectRequest.addWhere)

    selectRequest
  }

  def aggregationToSelectRequest(groupByList: Seq[NamedExpression],
                                 aggregates: Seq[AggregateExpression],
                                 source: TiDBRelation,
                                 selectRequest: TiSelectRequest = new TiSelectRequest): TiSelectRequest = {
    aggregates.foreach(expr =>
      expr match {
        case aggExpr: AggregateExpression =>
          aggExpr.aggregateFunction match {
            case Average(_) =>
              assert(false, "Should never be here")
            case Sum(BasicExpression(arg)) => {
              selectRequest.addAggregate(new TiSum(arg),
                fromSparkType(aggExpr.aggregateFunction.dataType))
            }
            case Count(args) => {
              val tiArgs = args.flatMap(BasicExpression.convertToTiExpr)
              selectRequest.addAggregate(new TiCount(tiArgs: _*))
            }
            case Min(BasicExpression(arg)) =>
              selectRequest.addAggregate(new TiMin(arg))

            case Max(BasicExpression(arg)) =>
              selectRequest.addAggregate(new TiMax(arg))

            case First(BasicExpression(arg), _) =>
              selectRequest.addAggregate(new TiFirst(arg))

            case _ => None
          }
      })
    groupByList.foreach(groupItem =>
      groupItem match {
        case BasicExpression(byExpr) =>
          selectRequest.addGroupByItem(TiByItem.create(byExpr, false))
        case _ => None
      }
    )

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

    val tiFilters:Seq[TiExpr] = pushdownFilters.map(expr => expr match { case BasicExpression(expr) => expr })
    val scanBuilder: ScanBuilder = new ScanBuilder
    val pkIndex: TiIndexInfo = TiIndexInfo.generateFakePrimaryKeyIndex(source.table)
    val scanPlan = scanBuilder.buildScan(JavaConversions.seqAsJavaList(tiFilters),
      pkIndex, source.table)

    selectRequest.addRanges(scanPlan.getKeyRanges)
    scanPlan.getFilters.toList.map(selectRequest.addWhere)

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
      val scan = toCoprocessorRDD(source, projectList.asInstanceOf[Seq[Attribute]], selectRequest)
      residualFilter.map(FilterExec(_, scan)).getOrElse(scan)
    } else {
      val scan = toCoprocessorRDD(source, (projectSet ++ filterSet).toSeq, selectRequest)
      ProjectExec(projectList, residualFilter.map(FilterExec(_, scan)).getOrElse(scan))
    }
  }

  // We do through similar logic with original Spark as in SparkStrategies.scala
  // Difference is we need to test if a sub-plan can be consumed all together by TiKV
  // and then we don't return (don't planLater) and plan the remaining all at once
  private def doPlan(cs: TiDBRelation, plan: LogicalPlan): Seq[SparkPlan] = {

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

    // TODO: This test should be done once for all children
    if (!isSupportedLogicalPlan(plan))
      Nil
    else
      plan match {
        // This is almost the same as Spark's original SpecialLimits logic
        // The difference is that we hijack the plan for push down
        // Limit + Sort can be consumed by coprocessor iff no aggregates
        // so we don't need to match it further
        case logical.ReturnAnswer(rootPlan) => rootPlan match {
          case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
            execution.TakeOrderedAndProjectExec(limit, order, child.output, toPhysicalRDD(cs, rootPlan)) :: Nil
          case logical.Limit(
          IntegerLiteral(limit),
          logical.Project(projectList, logical.Sort(order, true, _))) =>
            execution.TakeOrderedAndProjectExec(
              limit, order, projectList, toPhysicalRDD(cs, rootPlan)) :: Nil
          case logical.Limit(IntegerLiteral(limit), _) =>
            execution.CollectLimitExec(limit, toPhysicalRDD(cs, rootPlan)) :: Nil
        }

          // Collapse filters and projections and push plan directly
        case PhysicalOperation(projectList, filters, LogicalRelation(cs: TiDBRelation, _, _)) =>
          pruneFilterProject(projectList, filters, cs, null) :: Nil

          // A fall-back for all logic in case nothing to push
        case LogicalRelation(_: CatalystSource, _, _) => toPhysicalRDD(cs, plan) :: Nil

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
        case PhysicalAggregation(
                    groupingExpressions,
                    aggregateExpressions,
                    resultExpressions,
                    TiAggregationProjection(filters, rel, source))
          if !aggregateExpressions.exists(_.isDistinct) =>
          var selReq: TiSelectRequest =
            projectFilterToSelectRequest(Seq.empty[NamedExpression], filters, source)
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
                  val promotedType = ref.dataType match {
                    case DoubleType | DecimalType.Fixed(_, _) | LongType => ref
                    case _ => Cast(ref, DoubleType)
                  }
                  val sumToPush = newAggregate(Sum(promotedType), aggExpr)
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
                                              cs,
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

          aggregate.AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            residualAggregateExpressions,
            rewrittenResultExpression,
            pruneFilterProject(Seq.empty[NamedExpression], filters, source, selReq))

        case _ => Nil
      }
  }
}


object TiAggregationProjection {
  type ReturnType = (Seq[Expression], LogicalPlan, TiDBRelation)

  def unapply(plan: LogicalPlan): Option[ReturnType] = {
    plan match {
      // Only push down aggregates projection when all filters can be applied and
      // all projection expressions are column references
      case PhysicalOperation(projects, filters, rel@LogicalRelation(source: TiDBRelation, _, _))
        if projects.forall(_.isInstanceOf[Attribute]) &&
          filters.forall(TiUtils.isSupportedFilter) =>
        Some((filters, rel, source))
      case _ => Option.empty[ReturnType]
    }
  }
}