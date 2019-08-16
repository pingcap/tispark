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
import org.apache.spark.sql.execution.{CoprocessorRDD, HandleRDDExec, RegionTaskExec, SparkPlan}
import org.apache.spark.sql.{BaseTiSparkTest, Dataset}
import org.scalatest.exceptions.TestFailedException

class BasePlanTest extends BaseTiSparkTest {
  def toPlan[T](df: Dataset[T]): SparkPlan = df.queryExecution.executedPlan

  def explain[T](df: Dataset[T]): Unit = df.explain

  val extractCoprocessorRDD: PartialFunction[SparkPlan, CoprocessorRDD] = {
    case plan: CoprocessorRDD => plan
  }
  val extractRegionTaskExec: PartialFunction[SparkPlan, RegionTaskExec] = {
    case plan: RegionTaskExec => plan
  }
  val extractHandleRDDExec: PartialFunction[SparkPlan, HandleRDDExec] = {
    case plan: HandleRDDExec => plan
  }

  val extractTiSparkPlan: PartialFunction[SparkPlan, SparkPlan] = {
    case plan: CoprocessorRDD => plan
    case plan: HandleRDDExec  => plan
  }

  val extractDAGRequest: PartialFunction[SparkPlan, TiDAGRequest] = {
    case plan: CoprocessorRDD => plan.dagRequest
    case plan: HandleRDDExec  => plan.dagRequest
  }

  private def extractIndexInfo(coprocessorRDD: CoprocessorRDD): TiIndexInfo =
    coprocessorRDD.dagRequest.getIndexInfo

  private def extractIndexInfo(handleRDDExec: HandleRDDExec): TiIndexInfo = {
    handleRDDExec.dagRequest.getIndexInfo
  }

  def extractTiSparkPlans[T](df: Dataset[T]): Seq[SparkPlan] = toPlan(df).collect {
    extractTiSparkPlan
  }

  def extractDAGRequests[T](df: Dataset[T]): List[TiDAGRequest] = {
    toPlan(df).collect { extractDAGRequest }.toList
  }

  def toCoprocessorRDDs[T](df: Dataset[T]): List[CoprocessorRDD] =
    toPlan(df).collect { extractCoprocessorRDD }.toList

  def toRegionTaskExecs[T](df: Dataset[T]): List[RegionTaskExec] =
    toPlan(df).collect { extractRegionTaskExec }.toList

  def toHandleRDDExecs[T](df: Dataset[T]): List[HandleRDDExec] =
    toPlan(df).collect { extractHandleRDDExec }.toList

  private def checkIndex(coprocessorRDD: CoprocessorRDD, index: String): Boolean = {
    extractIndexInfo(coprocessorRDD).getName.equalsIgnoreCase(index)
  }

  private def checkIndex(handleRDDExec: HandleRDDExec, index: String): Boolean = {
    extractIndexInfo(handleRDDExec).getName.equalsIgnoreCase(index)
  }

  private def checkIndex(plan: SparkPlan, index: String): Boolean = plan match {
    case p: CoprocessorRDD => checkIndex(p, index)
    case p: HandleRDDExec  => checkIndex(p, index)
    case _                 => false
  }

  def checkIndex[T](df: Dataset[T], index: String): Boolean = {
    extractTiSparkPlans(df).exists(checkIndex(_, index))
  }

  private def getIndexScanType(dagRequest: TiDAGRequest): IndexScanType = {
    dagRequest.getIndexScanType
  }

  private def getIndexScanType(coprocessorRDD: CoprocessorRDD): IndexScanType = {
    getIndexScanType(coprocessorRDD.dagRequest)
  }

  private def getIndexScanType(handleRDDExec: HandleRDDExec): IndexScanType = {
    getIndexScanType(handleRDDExec.dagRequest)
  }

  private def checkIndexScanType(plan: SparkPlan, indexScanType: IndexScanType): Boolean =
    plan match {
      case p: CoprocessorRDD => getIndexScanType(p).equals(indexScanType)
      case p: HandleRDDExec  => getIndexScanType(p).equals(indexScanType)
      case _                 => false
    }

  /**
   * Explain dataset and fail the test with message
   */
  private def fail[T](df: Dataset[T], message: String): Unit = {
    df.explain
    fail(message)
  }

  private def fail[T](df: Dataset[T], message: String, throwable: Throwable): Unit = {
    df.explain
    fail(message, throwable)
  }

  private def checkIndexScanType[T](df: Dataset[T],
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
    } else if (filteredRequests.size > 1) {
      fail(df, s"Multiple TiSpark plan contains desired table $tableName")
    } else if (!tiSparkPlans.exists(checkIndexScanType(_, indexScanType))) {
      fail(
        df,
        s"Index scan type not match: ${filteredRequests.head.getIndexScanType}, expected $indexScanType"
      )
    }
  }

  def checkIsTableScan[T](df: Dataset[T], tableName: String): Unit =
    checkIndexScanType(df, tableName, IndexScanType.TABLE_SCAN)

  def checkIsCoveringIndexScan[T](df: Dataset[T], tableName: String): Unit =
    checkIndexScanType(df, tableName, IndexScanType.COVERING_INDEX_SCAN)

  def checkIsIndexScan[T](df: Dataset[T], tableName: String): Unit =
    checkIndexScanType(df, tableName, IndexScanType.INDEX_SCAN)

  def getEstimatedRowCount[T](df: Dataset[T], tableName: String): Double =
    extractTiSparkPlans(df).collect { extractDAGRequest }.head.getEstimatedCount

  def checkEstimatedRowCount[T](df: Dataset[T], tableName: String, answer: Double): Unit = {
    val estimatedRowCount = getEstimatedRowCount(df, tableName)
    assert(estimatedRowCount === answer)
  }


}
