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

import java.util

import com.pingcap.tikv._
import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._


class TiRDD(val dagRequest: TiDAGRequest,
            val tiConf: TiConfiguration,
            val tableRef: TiTableReference,
            val ts: TiTimestamp,
            sc: SparkContext)
  extends RDD[Row](sc, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val session: TiSession = TiSession.create(tiConf)
  @transient lazy val (fieldsType: List[DataType], rowTransformer: RowTransformer) = initializeSchema
  @transient lazy val snapshot: Snapshot = session.createSnapshot(ts)

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    dagRequest.bind
    // bypass, sum return a long type
    val tiPartition: TiPartition = split.asInstanceOf[TiPartition]
    val iterator: util.Iterator[TiRow] = snapshot.select(dagRequest, split.asInstanceOf[TiPartition].task)
    val finalTypes: List[DataType] = rowTransformer.getTypes.toList

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
    val keyWithRegionTasks = RangeSplitter.newSplitter(session.getRegionManager)
                 .splitRangeByRegion(dagRequest.getRanges)

    keyWithRegionTasks.zipWithIndex.map{
      case (task, index) => new TiPartition(index, task)
    }.toArray
  }
}
