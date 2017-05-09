package org.apache.spark.sql

import com.pingcap.tispark.TiCoprocessorOperation
import com.pingcap.tispark.TiCoprocessorOperation.CoprocessorReq
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{RDDConversions, RDDScanExec, SparkPlan}


class TiStrategy(context: SQLContext) extends Strategy with Logging {
  val sqlConf = context.conf

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case TiCoprocessorOperation(ret: CoprocessorReq) => {
      // Currently Spark does not support plan exploration so only one returned
      planCoprocessor(plan, ret) :: Nil
    }
    case _ => Nil
  }

  def planCoprocessor(plan: LogicalPlan, copInfo: CoprocessorReq): SparkPlan = {
    val internalRdd = RDDConversions.rowToRowRdd(copInfo._3.buildScan(copInfo),
      plan.schema.fields.map(_.dataType))
    RDDScanExec(plan.output, internalRdd, "TiSource")
  }
}
