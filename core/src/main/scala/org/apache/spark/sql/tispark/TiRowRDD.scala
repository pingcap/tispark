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
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tispark.listener.CacheInvalidateListener
import com.pingcap.tispark.utils.TiConverter
import com.pingcap.tispark.{TiPartition, TiTableReference}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TiRowRDD(override val dagRequest: TiDAGRequest,
               override val physicalId: Long,
               override val tiConf: TiConfiguration,
               override val tableRef: TiTableReference,
               @transient private val session: TiSession,
               @transient private val sparkSession: SparkSession)
    extends TiRDD(dagRequest, physicalId, tiConf, tableRef, session, sparkSession) {

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
    dagRequest.resolve()

    // bypass, sum return a long type
    private val tiPartition = split.asInstanceOf[TiPartition]
    private val session = TiSession.getInstance(tiConf)
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

    override def next(): Row = TiConverter.toSparkRow(iterator.next, rowTransformer)
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[TiPartition].tasks.head.getHost :: Nil
}
