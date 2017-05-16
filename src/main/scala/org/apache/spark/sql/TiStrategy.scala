package org.apache.spark.sql


import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, _}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Divide, Expression, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.planning.{PhysicalAggregation, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{RDDConversions, RDDScanExec, SparkPlan, aggregate}
import org.apache.spark.sql.sources.CatalystSource
import com.pingcap.tispark.TiUtils._

import scala.collection.mutable


class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf = context.conf


  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val relations = plan.collect({ case p => p })
      .filter(_.isInstanceOf[LogicalRelation])
      .map(_.asInstanceOf[LogicalRelation])
      .map(_.relation)
      .toList

    if (relations.isEmpty || relations.exists(!_.isInstanceOf[CatalystSource])) {
      Nil
    } else {
      val sources = relations.map(_.asInstanceOf[CatalystSource])
      val source = sources.head
      val partitionedExecution = source.isMultiplePartitionExecution(sources)
      if (partitionedExecution) planPartitioned(source, plan)
      else planNonPartitioned(source, plan)
    }
  }

  private def toPhysicalRDD(cs: CatalystSource, plan: LogicalPlan): SparkPlan = {
    val rdd = cs.logicalPlanToRDD(plan)
    val internalRdd = RDDConversions.rowToRowRdd(rdd,
      plan.schema.fields.map(_.dataType))

    RDDScanExec(plan.output, internalRdd, "CatalystSource")
  }

  private def planNonPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] =
    if (isSupportedLogicalPlan(plan)) {
      toPhysicalRDD(cs, plan) :: Nil
    } else {
      Nil
    }

  // We do through similar logic with original Spark as in SparkStrategies.scala
  // Difference is we need to test if a sub-plan can be consumed all together by TiKV
  // and then we don't return (don't planLater) and plan the remaining all at once
  private def planPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] = {

    val aliasMap = mutable.HashMap[Expression, Alias]()
    val avgRewriteMap = mutable.HashMap[Attribute, List[AggregateExpression]]()

    def toAlias(expr: Expression) = aliasMap.getOrElseUpdate(expr, Alias(expr, expr.toString)())

    def newAggregate(aggFunc: AggregateFunction,
                     originalAggExpr: AggregateExpression) =
      AggregateExpression(aggFunc, originalAggExpr.mode, originalAggExpr.isDistinct, originalAggExpr.resultId)

    // TODO: This test should be done once for all children
    if (!isSupportedLogicalPlan(plan))
      Nil
    else
      plan match {
        // This is almost the same as Spark's original SpecialLimits logic
        // The difference is that we hijack the plan for pushdown
        // Limit + Sort can be consumed by coprocessor iff no aggregates
        // so we don't need to match it further
        case logical.ReturnAnswer(rootPlan) => rootPlan match {
          case logical.Limit(IntegerLiteral(limit), logical.Sort(order, true, child)) =>
            execution.TakeOrderedAndProjectExec(limit, order, child.output, toPhysicalRDD(cs, child)) :: Nil
          case logical.Limit(
          IntegerLiteral(limit),
          logical.Project(projectList, logical.Sort(order, true, child))) =>
            execution.TakeOrderedAndProjectExec(
              limit, order, projectList, toPhysicalRDD(cs, child)) :: Nil
          case logical.Limit(IntegerLiteral(limit), child) =>
            execution.CollectLimitExec(limit, toPhysicalRDD(cs, child)) :: Nil
        }

          // Collapse filters and projections and push plan directly
        case PhysicalOperation(_, _, LogicalRelation(_: CatalystSource, _, _)) =>
          toPhysicalRDD(cs, plan) :: Nil

          // A fall-back for all logic in case nothing to push
        case LogicalRelation(_: CatalystSource, _, _) => toPhysicalRDD(cs, plan) :: Nil

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
                  avgRewriteMap(aggExpr.resultAttribute) = List(sumToPush, countFunc)
                  List(newAggregate(Sum(toAlias(sumToPush).toAttribute), aggExpr),
                    newAggregate(Count(toAlias(countFunc).toAttribute), aggExpr))

                case _ => aggExpr :: Nil
              }
          }


          val pushdownAggregates = aggregateExpressions.flatMap {
            aggExpr =>
              avgRewriteMap
                .getOrElse(aggExpr.resultAttribute, List(aggExpr))
                .map(x => toAlias(x))
          }

          val pushDownPlan = logical.Aggregate(
            groupingExpressions,
            pushdownAggregates ++ groupingExpressions,
            child)

          val rewrittenResultExpression = resultExpressions.map(
            expr => expr.transformDown {
              case aggExpr: AggregateExpression
                if (avgRewriteMap.contains(aggExpr.resultAttribute)) => {
                // Replace the original Average expression with Div of Alias
                val sumCountPair = avgRewriteMap(aggExpr.resultAttribute)

                Divide(Alias(sumCountPair(0), sumCountPair(0).toString)(),
                  Alias(sumCountPair(1), sumCountPair(1).toString)())
              }
              case other => other
            }.asInstanceOf[NamedExpression]
          )

          aggregate.AggUtils.planAggregateWithoutDistinct(
            groupingExpressions,
            residualAggregateExpressions,
            rewrittenResultExpression,
            toPhysicalRDD(cs, pushDownPlan))

        case _ => Nil
      }
  }
}
