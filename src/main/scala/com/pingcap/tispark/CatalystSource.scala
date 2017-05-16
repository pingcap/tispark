package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, TiStrategyContext}
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
    * Takes a logical plan and returns an RDD[InternalRow].
    *
    * Implementations can assume that [[isSupportedLogicalPlan()]]
    * was called before this method.
    *
    * @param plan Logical plan.
    * @return
    */
  def logicalPlanToRDD(plan: TiStrategyContext): RDD[Row]
  def logicalPlanToRDD(plan: LogicalPlan): RDD[Row]

}
