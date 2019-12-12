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

import com.pingcap.tikv.columnar.TiColumnarBatchHelper
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.operation.iterator.CoprocessorIterator
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.utils.DoubleReadUtils
import com.pingcap.tispark.{TiPartition, TiTableReference}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

import scala.collection.JavaConversions._

/**
 * RDD used for retrieving handles from TiKV. Result is arranged as
 * {{{
 *   RegionId(long):[handle1, handle2, handle3...](long[])
 * }}}
 * K-V pair, the key is regionId which stands for the id of a region in TiKV, value
 * is a list of primitive long which represents the handles lie in that region.
 *
 */
class TiHandleRDD(override val dagRequest: TiDAGRequest,
                  override val physicalId: Long,
                  override val tiConf: TiConfiguration,
                  override val tableRef: TiTableReference,
                  @transient private val session: TiSession,
                  @transient private val sparkSession: SparkSession)
    extends TiRDD(dagRequest, physicalId, tiConf, tableRef, session, sparkSession) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
    new Iterator[ColumnarBatch] {
      checkTimezone()

      dagRequest.resolve()
      private val tiPartition = split.asInstanceOf[TiPartition]
      private val session = TiSession.getInstance(tiConf)
      private val snapshot = session.createSnapshot(dagRequest.getStartTs)
      private[this] val tasks = tiPartition.tasks

      private val handleIterator = snapshot.indexHandleRead(dagRequest, tasks)

      override def hasNext: Boolean = {
        // Kill the task in case it has been marked as killed.
        if (context.isInterrupted()) {
          throw new TaskKilledException
        }
        handleIterator.hasNext
      }

      override def next(): ColumnarBatch = {
        val next = handleIterator.next
        val regionTasks =
          DoubleReadUtils.generateIndexTasks(next, session.getRegionManager, physicalId)
        val tiChunk = CoprocessorIterator
          .getTiChunkIterator(dagRequest, regionTasks, session, next.size())
          .next()
        TiColumnarBatchHelper.createColumnarBatch(tiChunk)
      }
    }.asInstanceOf[Iterator[InternalRow]]
}
