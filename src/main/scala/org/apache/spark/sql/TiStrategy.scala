package org.apache.spark.sql


import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Final}
import org.apache.spark.sql.catalyst.expressions.{Attribute, IntegerLiteral, NamedExpression}
import org.apache.spark.sql.catalyst.plans.{PartialAggregation, logical}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{RDDConversions, RDDScanExec, SparkPlan, aggregate}
import org.apache.spark.sql.sources.CatalystSource


class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf = context.conf

  // scalastyle:off cyclomatic.complexity
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
      partitionedExecution match {
        case false => planNonPartitioned(source, plan)
        case true => planPartitioned(source, plan)
      }
    }
  }
  // scalastyle:on cyclomatic.complexity
  private def toPhysicalRDD(cs: CatalystSource, plan: LogicalPlan): SparkPlan = {
    val rdd = cs.logicalPlanToRDD(plan)
    val internalRdd = RDDConversions.rowToRowRdd(rdd,
      plan.schema.fields.map(_.dataType))

    RDDScanExec(plan.output, internalRdd, "CatalystSource")
  }

  private def planNonPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] =
    if (cs.supportsLogicalPlan(plan)) {
      toPhysicalRDD(cs, plan) :: Nil
    } else {
      Nil
    }



  // scalastyle:off
  private def planPartitioned(cs: CatalystSource, plan: LogicalPlan): Seq[SparkPlan] = {

    @inline def isSupported(p: LogicalPlan, nonGlobal: LogicalPlan): Boolean =
      !containsGlobalOperators(nonGlobal) && cs.supportsLogicalPlan(p)
    /**
      * Before the Spark 1.6 migration the CatalystSource Strategy also distinguished in
      * cases the query asked for DISTINCT elements. However, this has been buried into aggregate
      * expressions - thus it is handled in the partial aggregation
      */
    plan match {
        // TODO: Port Limit logic for 2.1+
      case partialAgg@PartialAggregation(
      finalGroupings,
      finalAggregates,
      partialGroupings,
      partialAggregates,
      aggregateFunctionToAttributeMap,
      resultExpressions,
      child) =>

        // compose plan that has to be done by the datasource
        // We need to append group by expressions to output schema
        // and make sure what coprocessor produce exact plan output
        val pushDownPlan =
          logical.Aggregate(partialGroupings, partialAggregates ++ partialGroupings, child)

        if (isSupported(pushDownPlan, child)) {
          // Only non-distinct aggregates can be pushed
          aggregate.AggUtils.planAggregateWithoutDistinct(
            finalGroupings,
            finalAggregates,
            resultExpressions,
            toPhysicalRDD(cs, pushDownPlan))
        } else {
          Nil
        }

      // partitioned and aggregates do not go together, should have matched in the
      // partial aggregation part before
      case _: logical.Aggregate =>
        Nil
      case _ if isSupported(plan, plan) =>
        toPhysicalRDD(cs, plan) :: Nil
      case _ =>
        Nil
    }
  }

  /**
    * Spark SQL optimizer converts [[logical.Distinct]] to a [[logical.Aggregate]]
    * grouping by all columns. This method detects such case.
    *
    * @param agg
    * @return
    */
  private def isDistinct(agg: logical.Aggregate): Boolean = {
    agg.child.output == agg.groupingExpressions && agg.child.output == agg.aggregateExpressions
  }

  private def containsGlobalOperators(plan: LogicalPlan): Boolean =
    plan
      .collect { case op => isGlobalOperation(op) }
      .exists { isGlobal => isGlobal }

  private def isGlobalOperation(op: LogicalPlan): Boolean =
    op match {
        // TODO: Add limit back for 2.0
      case _: logical.Sort => true
      case _: logical.Distinct => true
      case _: logical.Intersect => true
      case _: logical.Except => true
      case _: logical.Aggregate => true
      case _ => false
    }

  /**
    * This emits a Spark Plan for partial aggregations taking Tungsten (if possible) into account.
    *
    * @param groupingExpressions grouping expressions of the final aggregate
    * @param aggregateExpressions aggregate expressions of the final aggregate
    * @param aggregateFunctionToAttribute mapping from attributes to aggregation functions
    * @param resultExpressions result expressions of this plan
    * @param pushedDownChild the child plan
    * @return Spark plan including pushed down parts
    */
  private def planAggregateWithoutDistinctWithPushdown(
                                                        groupingExpressions: Seq[NamedExpression],
                                                        aggregateExpressions: Seq[AggregateExpression],
                                                        aggregateFunctionToAttribute: Map[(AggregateFunction, Boolean), Attribute],
                                                        resultExpressions: Seq[NamedExpression],
                                                        pushedDownChild: SparkPlan): Seq[SparkPlan] = {
    aggregate.AggUtils.planAggregateWithoutDistinct(
      groupingExpressions,
      aggregateExpressions,
      resultExpressions,
      pushedDownChild)
  }
}
