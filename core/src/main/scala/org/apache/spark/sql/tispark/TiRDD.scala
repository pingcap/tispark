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
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.{Converter, DataType}
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tispark.listener.CacheInvalidateListener
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.{TiPartition, TiSessionCache, TiTableReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TiRDD(val dagRequest: TiDAGRequest,
            val physicalId: Long,
            val tiConf: TiConfiguration,
            val tableRef: TiTableReference,
            @transient private val session: TiSession,
            @transient private val sparkSession: SparkSession)
    extends RDD[Row](sparkSession.sparkContext, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val (_: List[DataType], rowTransformer: RowTransformer) =
    initializeSchema()

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  // cache invalidation call back function
  // used for driver to update PD cache
  private val callBackFunc = CacheInvalidateListener.getInstance()

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    if (!tiConf.checkLocalTimeZone()) {
      throw new TiInternalException(
        "timezone are different! dirver: " + tiConf.getLocalTimeZone + " executor:" + Converter.getLocalTimezone +
        " please set spark.driver.extraJavaOptions=-Duser.timezone=GMT+8 and spark.executor.extraJavaOptions -Duser.timezone=GMT+8"
      )
    }

    dagRequest.resolve()

    // bypass, sum return a long type
    private val tiPartition = split.asInstanceOf[TiPartition]
    private val session = TiSessionCache.getSession(tiConf)
    session.injectCallBackFunc(callBackFunc)
    private val snapshot = session.createSnapshot(dagRequest.getStartTs)
    private[this] val tasks = tiPartition.tasks

    private val iterator = snapshot.tableRead(dagRequest, tasks)

    override def hasNext: Boolean = {
      // Kill the task in case it has been marked as killed. This logic is from
      // Interrupted Iterator, but we inline it here instead of wrapping the iterator in order
      // to avoid performance overhead.
      if (context.isInterrupted()) {
        throw new TaskKilledException
      }
      iterator.hasNext
    }

    override def next(): Row = TiUtil.toSparkRow(iterator.next, rowTransformer)
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[TiPartition].tasks.head.getHost :: Nil

  override protected def getPartitions: Array[Partition] = {
    val keyWithRegionTasks = RangeSplitter
      .newSplitter(session.getRegionManager)
      .splitRangeByRegion(dagRequest.getRangesByPhysicalId(physicalId))

    val hostTasksMap = new mutable.HashMap[String, mutable.Set[RegionTask]]
    with mutable.MultiMap[String, RegionTask]

    var index = 0
    val result = new ListBuffer[TiPartition]
    for (task <- keyWithRegionTasks) {
      hostTasksMap.addBinding(task.getHost, task)
      val tasks = hostTasksMap(task.getHost)
      result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
      index += 1
      hostTasksMap.remove(task.getHost)
    }
    // add rest
    for (tasks <- hostTasksMap.values) {
      result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
      index += 1
    }
    result.toArray
  }
}
