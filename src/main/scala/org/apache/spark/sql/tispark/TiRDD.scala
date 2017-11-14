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

import java.util

import com.pingcap.tikv._
import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tispark.{TiConfigConst, TiPartition, TiSessionCache, TiTableReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer, MultiMap, Set}


class TiRDD(val dagRequest: TiDAGRequest,
            val tiConf: TiConfiguration,
            val tableRef: TiTableReference,
            val ts: TiTimestamp,
            @transient private val session: TiSession,
            @transient private val sparkSession: SparkSession)
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val (fieldsType: List[DataType], rowTransformer: RowTransformer) = initializeSchema

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    dagRequest.resolve()

    // bypass, sum return a long type
    val tiPartition = split.asInstanceOf[TiPartition]
    val session: TiSession = TiSessionCache.getSession(tiPartition.appId, tiConf)
    val snapshot: Snapshot = session.createSnapshot(ts)

    val iterator = snapshot.tableRead(dagRequest, split.asInstanceOf[TiPartition].tasks.asJava)
    val finalTypes = rowTransformer.getTypes.toList

    def toSparkRow(row: TiRow): Row = {
      val transRow = rowTransformer.transform(row)
      val rowArray = new Array[Any](finalTypes.size)

      for (i <- 0 until transRow.fieldCount) {
        rowArray(i) = transRow.get(i, finalTypes(i))
      }

      Row.fromSeq(rowArray)
    }

    override def hasNext: Boolean = iterator.hasNext

    override def next(): Row = toSparkRow(iterator.next)
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[TiPartition].tasks(0).getHost :: Nil

  override protected def getPartitions: Array[Partition] = {
    val conf = sparkSession.conf
    val keyWithRegionTasks = RangeSplitter.newSplitter(session.getRegionManager)
                 .splitRangeByRegion(dagRequest.getRanges,
                                     conf.get(TiConfigConst.TABLE_SCAN_SPLIT_FACTOR, "1").toInt)

    val taskPerSplit = conf.get(TiConfigConst.TASK_PER_SPLIT, "1").toInt
    val taskMap = new HashMap[String, Set[RegionTask]] with MultiMap[String, RegionTask]

    var index = 0
    val result = new ListBuffer[TiPartition]
    for (task <- keyWithRegionTasks) {
      taskMap.addBinding(task.getHost, task)
      val tasks = taskMap.get(task.getHost).get
      if (tasks.size >= taskPerSplit) {
        result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
        index += 1
        taskMap.remove(task.getHost)

      }
    }
    // add rest
    for (tasks <- taskMap.values) {
      result.append(new TiPartition(index, tasks.toSeq, sparkContext.applicationId))
      index += 1
    }
    result.toArray
  }
}
