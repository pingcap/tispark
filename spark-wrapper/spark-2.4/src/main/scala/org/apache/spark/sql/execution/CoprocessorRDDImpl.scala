/*
 * Copyright 2020 PingCAP, Inc.
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

package org.apache.spark.sql.execution

import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.tispark.TiRDD

import scala.collection.mutable

case class ColumnarCoprocessorRDDImpl(
    output: Seq[Attribute],
    override val tiRDDs: List[TiRDD],
    fetchHandle: Boolean)
    extends ColumnarCoprocessorRDD
    with ColumnarBatchScan {
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    Seq(sparkContext.union(internalRDDs))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (!fetchHandle) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      sparkContext.union(internalRDDs)
    }
  }

  override def simpleString: String = verboseString

  override def verboseString: String =
    if (tiRDDs.lengthCompare(1) > 0) {
      val b = new mutable.StringBuilder()
      b.append(s"TiSpark $nodeName on partition table:\n")
      tiRDDs.zipWithIndex.map {
        case (_, i) => b.append(s"partition p$i")
      }
      b.append(s"with dag request: $dagRequest")
      b.toString()
    } else {
      s"${dagRequest.getStoreType.name()} $nodeName{$dagRequest}" +
        s"${TiUtil.getReqEstCountStr(dagRequest)}"
    }
}

case class ColumnarRegionTaskExecImpl(
    child: SparkPlan,
    output: Seq[Attribute],
    chunkBatchSize: Int,
    dagRequest: TiDAGRequest,
    tiConf: TiConfiguration,
    ts: TiTimestamp,
    @transient private val session: TiSession,
    @transient private val sparkSession: SparkSession)
    extends ColumnarRegionTaskExec
    with ColumnarBatchScan {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numHandles" -> SQLMetrics
      .createMetric(sparkContext, "number of handles used in double scan"),
    "numDowngradedTasks" -> SQLMetrics.createMetric(sparkContext, "number of downgraded tasks"),
    "numIndexScanTasks" -> SQLMetrics
      .createMetric(sparkContext, "number of index double read tasks"),
    "numRegions" -> SQLMetrics.createMetric(sparkContext, "number of regions"),
    "numIndexRangesScanned" -> SQLMetrics
      .createMetric(sparkContext, "number of index ranges scanned"),
    "numDowngradeRangesScanned" -> SQLMetrics
      .createMetric(sparkContext, "number of downgrade ranges scanned"))

  override def simpleString: String = verboseString

  override def verboseString: String =
    s"TiSpark $nodeName{downgradeThreshold=$downgradeThreshold,downgradeFilter=${dagRequest.getFilters}"

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD())
}
