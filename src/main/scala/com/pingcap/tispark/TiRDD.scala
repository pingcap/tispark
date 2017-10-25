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

package com.pingcap.tispark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext}


class TiRDD(val selectReq: TiSelectRequest,
            val tiConf: TiConfiguration,
            val tableRef: TiTableReference,
            val ts: TiTimestamp,
            @transient sparkSession: SparkSession)
  extends RDD[Row](sparkSession.sparkContext, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val (fieldsType: List[DataType], rowTransformer: RowTransformer) = initializeSchema

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(selectReq)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    selectReq.resolve
    // bypass, sum return a long type
    val appId = sparkSession.sparkContext.applicationId
    val tiPartition = split.asInstanceOf[TiPartition]
    val session: TiSession = TiSessionCache.getSession(appId, tiPartition.execId, tiConf)
    val snapshot: Snapshot = session.createSnapshot(ts)

    val iterator = snapshot.tableRead(selectReq, split.asInstanceOf[TiPartition].task)
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
    split.asInstanceOf[TiPartition].task.getHost :: Nil

  override protected def getPartitions: Array[Partition] = {
    val conf = sparkSession.conf
    val keyWithRegionTasks = RangeSplitter.newSplitter(session.getRegionManager)
                 .splitRangeByRegion(selectReq.getRanges,
                                     conf.get(TiConfigConst.TABLE_SCAN_SPLIT_FACTOR, "1").toInt)

    val execId = TiUtils.allocExecId()
    keyWithRegionTasks.zipWithIndex.map {
      case (task, index) => new TiPartition(index, task, execId)
    }.toArray
  }
}
