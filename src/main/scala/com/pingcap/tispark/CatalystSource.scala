package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * A relation supporting full logical plans straight from Catalyst.
  */
trait CatalystSource {

  /**
    * Given a sequence of relations, return true if executing a query
    * on them would result in a query issued to multiple partitions.
    * Returns false if it would result in a query to a single partition
    * (and therefore provides global results).
    *
    * Note that the same relation could be repeated in the same sequence
    * (e.g. a self-join).
    *
    * @param relations Relations participating in a query.
    * @return
    */
  def isMultiplePartitionExecution(relations: Seq[CatalystSource]): Boolean

  /**
    * Checks if the logical plan is fully supported by this relation.
    *
    * Implementations can assume that [[CatalystSource.isMultiplePartitionExecution()]]
    * was called before this method and that [[LogicalPlan]] was modified accordingly
    *
    * @param plan Logical plan.
    * @return
    */
  def supportsLogicalPlan(plan: LogicalPlan): Boolean

  /**
    * Checks if an expression is supported. This might be used by the
    * planner to maximize the portion of the logical plan that is
    * pushed down.
    *
    * It is safe to always return false here ([[CatalystSource.supportsLogicalPlan()]]
    * will be called anyway, but giving proper answers here will help getting
    * more aggregations pushed down.
    *
    * @param expr Expression.
    * @return
    */
  def supportsExpression(expr: Expression): Boolean

  /**
    * Takes a logical plan and returns an RDD[InternalRow].
    *
    * Implementations can assume that [[CatalystSource.supportsLogicalPlan()]]
    * was called before this method.
    *
    * @param plan Logical plan.
    * @return
    */
  def logicalPlanToRDD(plan: LogicalPlan): RDD[Row]

}
