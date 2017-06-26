package org.apache.spark.sql.sources

import com.pingcap.tikv.meta.TiTableInfo
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, TiStrategyContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
  * A relation supporting full logical plans straight from Catalyst.
  */
trait CatalystSource {
  /**
    * Takes a logical plan and returns an RDD[InternalRow].
    *
    * Implementations can assume that [[isSupportedLogicalPlan()]]
    * was called before this method.
    *
    * @param plan Logical plan.
    * @return
    */
  def logicalPlanToRDD(plan: LogicalPlan): RDD[Row]

  def tableInfo : TiTableInfo

}
