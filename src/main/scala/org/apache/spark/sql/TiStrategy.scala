package org.apache.spark.sql

import com.pingcap.tispark.TiCoprocessorOperation
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan


class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf = context.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case tiAggregate@TiCoprocessorOperation(
        partialGroupingExprs,
        partialAggExprs,
        finalGroupingExprs,
        finalAggExprs,
        childPlan) => {
      logInfo("Test apply in tiStrategy")
      Nil
    }
    case _ => Nil
  }
}
