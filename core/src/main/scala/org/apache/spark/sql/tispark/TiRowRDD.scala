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
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tispark.listener.CacheInvalidateListener
import com.pingcap.tispark.utils.{TiConverter, TiUtil}
import com.pingcap.tispark.{TiConfigConst, TiPartition, TiTableReference}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{Partition, TaskContext, TaskKilledException}
import org.slf4j.Logger

import scala.collection.JavaConversions._

class TiRowRDD(override val dagRequest: TiDAGRequest,
               override val physicalId: Long,
               override val tiConf: TiConfiguration,
               override val tableRef: TiTableReference,
               @transient private val session: TiSession,
               @transient private val sparkSession: SparkSession)
    extends TiRDD(dagRequest, physicalId, tiConf, tableRef, session, sparkSession) {

  protected val logger: Logger = log

  override protected def getPartitions: Array[Partition] = {
    val partitions = super.getPartitions

    if (!dagRequest.getUseTiFlash)
      return partitions

    logger.info("testing tiflash logic")
    val regionMgr = session.getRegionManager
    partitions.map(p => {
      val tiPartition = p.asInstanceOf[TiPartition]
      val tasks = tiPartition.tasks
        .map(task => {
          val learnerList = task.getRegion.getLearnerList
          val learnerStore = learnerList
            .collectFirst {
              case peer =>
                val store = regionMgr.getStoreById(peer.getStoreId)
                if (store.getLabelsList
                      .exists(
                        label =>
                          label.getKey == tiConf.getTiFlashLabelKey && label.getValue == tiConf.getTiFlashLabelValue
                      )) {
                  val builder = store.toBuilder
                  val address = builder.getAddress
                  val parts = address.split(":")
                  if (parts.size != 2)
                    throw new Exception("Invalid learner peer address: " + address)
                  // Convention: learner port + 1 is the coprocessor port of TiFlash.
                  builder.setAddress(parts(0) + ":" + (Integer.valueOf(parts(1)) + 1))
                  builder.build()
                } else {
                  null
                }
            }
            .getOrElse(
              throw new Exception(
                "No TiFlash store [" + tiConf.getTiFlashLabelKey + ":" + tiConf.getTiFlashLabelValue + "] found for region " + task.getRegion.getId
              )
            )
          RegionTask.newInstance(task.getRegion, learnerStore, task.getRanges)
        })
      new TiPartition(tiPartition.idx, tasks, tiPartition.appId)
    })
  }

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
