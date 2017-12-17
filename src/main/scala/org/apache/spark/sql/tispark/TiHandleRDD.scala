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

import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.codec.TableCodec
import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.{TiConfigConst, TiPartition, TiSessionCache, TiTableReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TiHandleRDD(val dagRequest: TiDAGRequest,
                  val tiConf: TiConfiguration,
                  val tableRef: TiTableReference,
                  val ts: TiTimestamp,
                  @transient private val session: TiSession,
                  @transient private val sparkSession: SparkSession)
    extends RDD[Row](sparkSession.sparkContext, Nil) {
  var lastHandelReadTimeMs: Long = 0

  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    new Iterator[Row] {
      dagRequest.resolve()
      // bypass, sum return a long type
      private val tiPartition = split.asInstanceOf[TiPartition]
      private val session = TiSessionCache.getSession(tiPartition.appId, tiConf)
      private val snapshot = session.createSnapshot(ts)
      private val iterator =
        snapshot.handleRead(dagRequest, split.asInstanceOf[TiPartition].tasks.asJava)
      private val tableId = dagRequest.getTableInfo.getId

      override def hasNext: Boolean = iterator.hasNext

      override def next(): Row = {
        val regionManager = session.getRegionManager
        val start = System.currentTimeMillis()
        val handle = iterator.next()
        lastHandelReadTimeMs = System.currentTimeMillis() - start
        val key = TableCodec.encodeRowKeyWithHandleBytes(tableId, handle)
        val region = regionManager.getRegionByKey(ByteString.copyFrom(key))

        // Returns Region:Handle K-V pair
        Row.apply(region.getId, handle)
      }
    }

  override protected def getPartitions: Array[Partition] = {
    val conf = sparkSession.conf
    val keyWithRegionTasks = RangeSplitter
      .newSplitter(session.getRegionManager)
      .splitRangeByRegion(
        dagRequest.getRanges,
        conf.get(TiConfigConst.TABLE_SCAN_SPLIT_FACTOR, "1").toInt
      )

    val taskPerSplit = conf.get(TiConfigConst.TASK_PER_SPLIT, "1").toInt
    val hostTasksMap = new mutable.HashMap[String, mutable.Set[RegionTask]]
    with mutable.MultiMap[String, RegionTask]

    var index = 0
    val result = new ListBuffer[TiPartition]
    for (task <- keyWithRegionTasks) {
      hostTasksMap.addBinding(task.getHost, task)
      val tasks = hostTasksMap(task.getHost)
      if (tasks.size >= taskPerSplit) {
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
}
