/*
 * Copyright 2017 PingCAP, Inc.
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

package org.apache.spark.sql.tispark

import com.pingcap.tikv._
import com.pingcap.tikv.exception.TiInternalException
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.types.Converter
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tispark.{TiPartition, TiTableReference}
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

abstract class TiRDD(
    val dagRequest: TiDAGRequest,
    val physicalId: Long,
    val tiConf: TiConfiguration,
    val tableRef: TiTableReference,
    @transient private val session: TiSession,
    @transient private val sparkSession: SparkSession)
    extends RDD[InternalRow](sparkSession.sparkContext, Nil) {

  private lazy val partitionPerSplit = tiConf.getPartitionPerSplit

  protected def checkTimezone(): Unit = {
    if (!tiConf.getLocalTimeZone.equals(Converter.getLocalTimezone)) {
      throw new TiInternalException(
        "timezone are different! driver: " + tiConf.getLocalTimeZone + " executor:" + Converter.getLocalTimezone +
          " please set user.timezone in spark.driver.extraJavaOptions and spark.executor.extraJavaOptions")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    val keyWithRegionTasks = RangeSplitter
      .newSplitter(session.getRegionManager)
      .splitRangeByRegion(dagRequest.getRangesByPhysicalId(physicalId), dagRequest.getStoreType)

    val hostTasksMap = new mutable.HashMap[String, mutable.Set[RegionTask]]
      with mutable.MultiMap[String, RegionTask]

    var index = 0
    val result = new ListBuffer[TiPartition]
    for (task <- keyWithRegionTasks) {
      hostTasksMap.addBinding(task.getHost, task)
      val tasks = hostTasksMap(task.getHost)
      if (tasks.size >= partitionPerSplit) {
        result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
        index += 1
        hostTasksMap.remove(task.getHost)
      }

    }
    // add rest
    for (tasks <- hostTasksMap.values) {
      result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
      index += 1
    }
    result.toArray
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[TiPartition].tasks.head.getHost :: Nil
}
