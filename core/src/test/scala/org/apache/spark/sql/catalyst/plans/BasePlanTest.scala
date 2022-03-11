/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql.catalyst.plans

import com.pingcap.tikv.meta.TiDAGRequest.IndexScanType
import com.pingcap.tikv.meta.{TiDAGRequest, TiIndexInfo}
import org.apache.spark.sql.execution.{ColumnarCoprocessorRDD, ColumnarRegionTaskExec, SparkPlan}
import org.apache.spark.sql.{BaseTiSparkTest, Dataset}

class BasePlanTest extends BaseTiSparkTest {
  val extractCoprocessorRDD: PartialFunction[SparkPlan, ColumnarCoprocessorRDD] = {
    case plan: ColumnarCoprocessorRDD => plan
  }
  val extractRegionTaskExec: PartialFunction[SparkPlan, ColumnarRegionTaskExec] = {
    case plan: ColumnarRegionTaskExec => plan
  }
  val extractTiSparkPlan: PartialFunction[SparkPlan, SparkPlan] = {
    case plan: ColumnarCoprocessorRDD => plan
    case plan: ColumnarRegionTaskExec => plan
  }
  val extractDAGRequest: PartialFunction[SparkPlan, TiDAGRequest] = {
    case plan: ColumnarRegionTaskExec => plan.dagRequest
    case plan: ColumnarCoprocessorRDD => plan.dagRequest
  }

  def explain[T](df: Dataset[T]): Unit = df.explain

  def extractDAGRequests[T](df: Dataset[T]): Seq[TiDAGRequest] =
    toPlan(df).collect { extractDAGRequest }

  def extractTiSparkPlans[T](df: Dataset[T]): Seq[SparkPlan] =
    toPlan(df).collect { extractTiSparkPlan }

  def extractCoprocessorRDDs[T](df: Dataset[T]): Seq[ColumnarCoprocessorRDD] =
    toPlan(df).collect { extractCoprocessorRDD }

  def extractRegionTaskExecs[T](df: Dataset[T]): List[ColumnarRegionTaskExec] =
    toPlan(df).collect { extractRegionTaskExec }.toList

  def checkIndex[T](df: Dataset[T], index: String): Unit = {
    if (!extractCoprocessorRDDs(df).exists(checkIndexName(_, index))) {
      df.explain
      fail(s"index not match, expected index $index")
    }
  }

  private def checkIndexName(coprocessorRDD: ColumnarCoprocessorRDD, index: String): Boolean =
    extractIndexInfo(coprocessorRDD).getName.equalsIgnoreCase(index)

  private def extractIndexInfo(coprocessorRDD: ColumnarCoprocessorRDD): TiIndexInfo =
    coprocessorRDD.dagRequest.getIndexInfo

  def checkIsTableScan[T](df: Dataset[T], tableName: String): Unit =
    checkIndexScanType(df, tableName, IndexScanType.TABLE_SCAN)

  private def checkIndexScanType[T](
      df: Dataset[T],
      tableName: String,
      indexScanType: IndexScanType): Unit = {
    val tiSparkPlans = extractTiSparkPlans(df)
    if (tiSparkPlans.isEmpty) {
      fail(df, "No TiSpark plans found in Dataset")
    }
    val filteredRequests = tiSparkPlans.collect { extractDAGRequest }.filter {
      _.getTableInfo.getName.equalsIgnoreCase(tableName)
    }
    if (filteredRequests.isEmpty) {
      fail(df, s"No TiSpark plan contains desired table $tableName")
    } else if (!tiSparkPlans.exists(checkIndexScanType(_, indexScanType))) {
      fail(
        df,
        s"Index scan type not match: ${filteredRequests.head.getIndexScanType}, expected $indexScanType")
    }
  }

  private def checkIndexScanType(plan: SparkPlan, indexScanType: IndexScanType): Boolean =
    plan match {
      case p: ColumnarCoprocessorRDD => getIndexScanType(p).equals(indexScanType)
      case _ => false
    }

  private def getIndexScanType(coprocessorRDD: ColumnarCoprocessorRDD): IndexScanType = {
    getIndexScanType(coprocessorRDD.dagRequest)
  }

  private def getIndexScanType(dagRequest: TiDAGRequest): IndexScanType = {
    dagRequest.getIndexScanType
  }

  /**
   * Explain dataset and fail the test with message
   */
  private def fail[T](df: Dataset[T], message: String): Unit = {
    df.explain
    fail(message)
  }

  def checkIsCoveringIndexScan[T](df: Dataset[T], tableName: String): Unit =
    checkIndexScanType(df, tableName, IndexScanType.COVERING_INDEX_SCAN)

  def checkIsIndexScan[T](df: Dataset[T], tableName: String): Unit =
    checkIndexScanType(df, tableName, IndexScanType.INDEX_SCAN)

  def checkEstimatedRowCount[T](df: Dataset[T], tableName: String, answer: Double): Unit = {
    val estimatedRowCount = getEstimatedRowCount(df, tableName)
    assert(estimatedRowCount === answer)
  }

  def getEstimatedRowCount[T](df: Dataset[T], tableName: String): Double =
    extractTiSparkPlans(df).collect { extractDAGRequest }.head.getEstimatedCount

  def toPlan[T](df: Dataset[T]): SparkPlan = df.queryExecution.executedPlan

  private def fail[T](df: Dataset[T], message: String, throwable: Throwable): Unit = {
    df.explain
    fail(message, throwable)
  }

}
