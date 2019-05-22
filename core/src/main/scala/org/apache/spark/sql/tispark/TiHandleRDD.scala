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

import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.{TiPartition, TiSessionCache, TiTableReference}
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * RDD used for retrieving handles from TiKV. Result is arranged as
 * {{{
 *   RegionId(long):[handle1, handle2, handle3...](long[])
 * }}}
 * K-V pair, the key is regionId which stands for the id of a region in TiKV, value
 * is a list of primitive long which represents the handles lie in that region.
 *
 */
class TiHandleRDD(val dagRequest: TiDAGRequest,
                  val physicalId: Long,
                  val tiConf: TiConfiguration,
                  val tableRef: TiTableReference,
                  val ts: TiTimestamp,
                  @transient private val session: TiSession,
                  @transient private val sparkSession: SparkSession)
    extends RDD[Row](sparkSession.sparkContext, Nil) {

  override def compute(split: Partition, context: TaskContext): Iterator[Row] =
    new Iterator[Row] {
      dagRequest.resolve()
      private val tiPartition = split.asInstanceOf[TiPartition]
      private val session = TiSessionCache.getSession(tiConf)
      private val snapshot = session.createSnapshot(ts)
      private[this] val tasks = tiPartition.tasks

      private val handleIterator = snapshot.indexHandleRead(dagRequest, tasks)
      private val regionManager = session.getRegionManager
      private lazy val handleList = {
        val lst = new TLongArrayList()
        handleIterator.asScala.foreach {
          // Kill the task in case it has been marked as killed. This logic is from
          // InterruptedIterator, but we inline it here instead of wrapping the iterator in order
          // to avoid performance overhead.
          if (context.isInterrupted()) {
            throw new TaskKilledException
          }
          lst.add(_)
        }
        lst
      }
      // Fetch all handles and group by region id
      private val regionHandleMap = RangeSplitter
        .newSplitter(regionManager)
        .groupByAndSortHandlesByRegionId(physicalId, handleList)
        .map(x => (x._1.first.getId, x._2))

      private val iterator = regionHandleMap.iterator

      override def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed.
        if (context.isInterrupted()) {
          throw new TaskKilledException
        }
        iterator.hasNext
      }

      override def next(): Row = {
        val next = iterator.next
        val regionId = next._1
        val handleList = next._2

        // Returns RegionId:[handle1, handle2, handle3...] K-V pair
        Row.apply(regionId, handleList.toArray())
      }
    }

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
