package org.apache.spark.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Divide, ExprId, Expression, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{RDDConversions, RDDScanExec, SparkPlan, aggregate}
import org.apache.spark.sql.sources.CatalystSource

import scala.collection.mutable

/**
  * We do a match for subplan where coprocessor can process
  * If it is not a valid TiKV sub-plan, return nil in chain
  * It's a valid plan iff the whole sub plan tree can be consumed
  */
class TiStrategies(context: SQLContext) extends Strategy with Logging {
  val tiContext = new TiStrategyContext()
  val tiStrategies = new LimitStrategy() :: new AggregateStrategy() :: new ProjectionFilterStrategy() :: Nil

  private def planInner(plan: LogicalPlan): SparkPlan = {
    var result:Seq[SparkPlan] = Nil
    tiStrategies.find{
      strategy =>
        result = strategy(plan)
        !result.isEmpty
    }
    if (result.isEmpty)
      null
    else
      result(0)
  }


  private def toPhysicalRDD(cs: CatalystSource, context: TiStrategyContext): SparkPlan = {
    val rdd = cs.logicalPlanToRDD(context)
    val internalRdd = RDDConversions.rowToRowRdd(rdd,
      context.schema.fields.map(_.dataType))

    RDDScanExec(context.output, internalRdd, "CatalystSource")
  }


  class AggregateStrategy extends Strategy with Logging {

    val aliasMap = mutable.HashMap[Expression, Alias]()
    val avgRewriteMap = mutable.HashMap[ExprId, List[AggregateExpression]]()
    val aggRewriteMap = mutable.HashMap[ExprId, AggregateExpression]()

    def toAlias(expr: Expression) = aliasMap.getOrElseUpdate(expr, Alias(expr, expr.toString)())

    def newAggregate(aggFunc: AggregateFunction,
                     originalAggExpr: AggregateExpression) = {
      val newExpr = AggregateExpression(aggFunc, originalAggExpr.mode, originalAggExpr.isDistinct, originalAggExpr.resultId)
      aggRewriteMap(originalAggExpr.resultId) = newExpr
      newExpr
    }

    def oldAggExprToNew(exprId: ExprId) = aggRewriteMap(exprId)

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalAggregation(
        groupingExpressions, aggregateExpressions, resultExpressions, child)
          if (!aggregateExpressions.exists(_.isDistinct)) =>
          val residualAggregateExpressions = aggregateExpressions.map {
            aggExpr =>
              aggExpr.aggregateFunction match {
                case Max(_) => newAggregate(Max(toAlias(aggExpr).toAttribute), aggExpr)
                case Min(_) => newAggregate(Min(toAlias(aggExpr).toAttribute), aggExpr)
                case Count(_) => newAggregate(Count(toAlias(aggExpr).toAttribute), aggExpr)
                case Sum(_) => newAggregate(Sum(toAlias(aggExpr).toAttribute), aggExpr)
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
                  val sumToPush = newAggregate(Sum(ref), aggExpr)
                  val countFunc = newAggregate(Count(ref), aggExpr)
                  avgRewriteMap(aggExpr.resultId) = List(sumToPush, countFunc)
                  List(newAggregate(Sum(toAlias(sumToPush).toAttribute), aggExpr),
                    newAggregate(Count(toAlias(countFunc).toAttribute), aggExpr))

                case _ => aggExpr :: Nil
              }
          }


          val pushdownAggregates = aggregateExpressions.flatMap {
            aggExpr =>
              avgRewriteMap
                .getOrElse(aggExpr.resultId, List(aggExpr))
                .map(x => toAlias(x))
          }

          val pushDownPlan = logical.Aggregate(
            groupingExpressions,
            pushdownAggregates ++ groupingExpressions,
            child)

          val rewrittenResultExpression:Seq[NamedExpression] = resultExpressions.map {
            expr: NamedExpression => expr.transformDown {
              // In Spark extractor [[PhysicalAggregation]] original Aggregates
              // have been replaced with AttributeReference towards Aggregates's output
              case expr: AttributeReference =>
                if (avgRewriteMap.contains(expr.exprId)) {
                  // Replace the original Average expression with Div of Alias
                  val sumCountPair = avgRewriteMap(expr.exprId)

                  Divide(sumCountPair(0).resultAttribute, sumCountPair(1).resultAttribute)
                } else if (aggRewriteMap.contains(expr.exprId)) {
                  // Original AggregateExpression has been replaced with
                  // new one with new reference to new sub-plan result
                  // so here we need a replacement for post-aggregate projection
                  // reference
                  oldAggExprToNew(expr.exprId).resultAttribute
                } else {
                  expr
                }
              case other => other
            }.asInstanceOf[NamedExpression]
          }

          val childPlan = planInner(child)
          if (childPlan != null) {
            tiContext.output = resultExpressions.map(_.toAttribute)
            tiContext.groupingExpressions = groupingExpressions
            tiContext.aggregations = pushdownAggregates
            aggregate.AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              residualAggregateExpressions,
              rewrittenResultExpression,
              childPlan)
          } else {
            Nil
          }

        case _ => Nil
      }
    }
  }

  class ProjectionFilterStrategy extends Strategy with Logging {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalOperation(projectList, filters, LogicalRelation(relation: CatalystSource, _, _)) =>
          tiContext.filters = filters
          tiContext.projections = projectList
          tiContext.output = projectList.map(_.toAttribute)
          toPhysicalRDD(relation, tiContext) :: Nil
        case _ => Nil
      }
    }
  }

  // This is almost the same as Spark's original SpecialLimits logic
  // The difference is that we hijack the plan for pushdown
  // Limit + Sort can be consumed by coprocessor iff no aggregates
  // so we don't need to match it further
  class LimitStrategy extends Strategy with Logging {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case logical.ReturnAnswer(rootPlan) => rootPlan match {
          case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
            val childPlan = planInner(child)
            if (childPlan != null) {
              tiContext.limit = limit
              tiContext.output = child.output
              execution.TakeOrderedAndProjectExec(limit, order, child.output, childPlan) :: Nil
            } else {
              Nil
            }
          case logical.Limit(
          IntegerLiteral(limit),
          logical.Project(projectList, logical.Sort(order, true, child))) =>
            val childPlan = planInner(child)
            if (childPlan != null) {
              tiContext.limit = limit
              tiContext.output = projectList.map(_.toAttribute)
              execution.TakeOrderedAndProjectExec(
                limit, order, projectList, childPlan) :: Nil
            } else {
              Nil
            }
          case logical.Limit(IntegerLiteral(limit), child) =>
            val childPlan = planInner(child)
            if (childPlan != null) {
              tiContext.limit = limit
              tiContext.output = childPlan.output
              execution.CollectLimitExec(limit, childPlan) :: Nil
            } else {
              Nil
            }
          case _ => Nil
        }
        case _ => Nil
      }
    }
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val sparkPlan = planInner(plan)
    if (sparkPlan == null) Nil
    else
      sparkPlan :: Nil
  }
}
