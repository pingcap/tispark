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

import com.pingcap.tikv._
import com.pingcap.tikv.meta.TiSelectRequest
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.{Partition, SparkContext, TaskContext}

import scala.collection.JavaConversions._


class TiRDD(val selectReq: TiSelectRequest, val options: TiOptions, @transient sc: SparkContext)
  extends RDD[Row](sc, Nil) {

  type TiRow = com.pingcap.tikv.row.Row

  @transient lazy val meta: MetaManager = new MetaManager(options.addresses)
  @transient lazy val cluster: TiCluster = meta.cluster
  @transient lazy val snapshot: Snapshot = cluster.createSnapshot()
  @transient lazy val (fieldsType: List[DataType], rowTransformer: RowTransformer) = initializeSchema

  def initializeSchema(): (List[DataType], RowTransformer) = {
    val schemaInferrer: SchemaInfer = SchemaInfer.create(selectReq)
    val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
    (schemaInferrer.getTypes.toList, rowTransformer)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row] {
    context.addTaskCompletionListener{ _ => cluster.close() }

    selectReq.bind
    // bypass, sum return a long type
    val tiPartition = split.asInstanceOf[TiPartition]
    val iterator = snapshot.select(selectReq, split.asInstanceOf[TiPartition].task)
    val finalTypes = rowTransformer.getTypes.toList

    def toSparkRow(row: TiRow): Row = {
      val transRow = rowTransformer.transform(row)
      val rowArray = new Array[Any](rowTransformer.getTypes.size)

      for (i <- 0 until transRow.fieldCount) {
        rowArray(i) = transRow.get(i, finalTypes(i))
      }

      Row.fromSeq(rowArray)
    }

    override def hasNext: Boolean = iterator.hasNext

    override def next(): Row = toSparkRow(iterator.next)
  }

  override protected def getPartitions: Array[Partition] = {
    val keyWithRegionTasks = RangeSplitter.newSplitter(cluster.getRegionManager)
                 .splitRangeByRegion(selectReq.getRanges)

    keyWithRegionTasks.zipWithIndex.map{
      case (task, index) => new TiPartition(index, task)
    }.toArray
  }
}
