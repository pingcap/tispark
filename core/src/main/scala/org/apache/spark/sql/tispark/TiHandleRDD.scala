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

import java.nio.ByteBuffer

import com.pingcap.tikv.codec.CodecDataOutput
import com.pingcap.tikv.columnar.{
  TiChunk,
  TiChunkColumnVector,
  TiColumnVector,
  TiColumnarBatchHelper
}
import com.pingcap.tikv.meta.TiDAGRequest
import com.pingcap.tikv.types.{ArrayType, DataType, IntegerType}
import com.pingcap.tikv.util.RangeSplitter
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.{TiPartition, TiTableReference}
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partition, TaskContext, TaskKilledException}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * RDD used for retrieving handles from TiKV. Result is arranged as
 * {{{
 *   RegionId(long):[handle1, handle2, handle3...](long[])
 * }}}
 * K-V pair, the key is regionId which stands for the id of a region in TiKV, value
 * is a list of primitive long which represents the handles lie in that region.
 *
 */
class TiHandleRDD(
    override val dagRequest: TiDAGRequest,
    override val physicalId: Long,
    val output: Seq[Attribute],
    override val tiConf: TiConfiguration,
    override val tableRef: TiTableReference,
    @transient private val session: TiSession,
    @transient private val sparkSession: SparkSession)
    extends TiRDD(dagRequest, physicalId, tiConf, tableRef, session, sparkSession) {

  private val outputTypes = output.map(_.dataType)
  private val converters =
    outputTypes.map(CatalystTypeConverters.createToCatalystConverter)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] =
    new Iterator[ColumnarBatch] {
      checkTimezone()

      private val tiPartition = split.asInstanceOf[TiPartition]
      private val session = TiSession.getInstance(tiConf)
      private val snapshot = session.createSnapshot(dagRequest.getStartTs)
      private[this] val tasks = tiPartition.tasks

      private val handleIterator = snapshot.indexHandleReadRow(dagRequest, tasks)
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

      override def next(): ColumnarBatch = {
        var numRows = 0
        val batchSize = 20480
        val cdi0 = new CodecDataOutput()
        val cdi1 = new CodecDataOutput()
        var offsets = new mutable.ArrayBuffer[Long]
        var curOffset = 0L
        while (hasNext && numRows < batchSize) {
          val next = iterator.next
          val regionId = next._1
          val handleList = next._2
          if (!handleList.isEmpty) {
            // Returns RegionId:[handle1, handle2, handle3...] K-V pair
//            val sparkRow = Row.apply(regionId, handleList.toArray())
//            TiUtil.rowToInternalRow(sparkRow, outputTypes, converters)
            cdi0.writeLong(regionId)
            cdi1.writeLong(handleList.size())
            for (i <- 0 until handleList.size()) {
              cdi1.writeLong(handleList.get(i))
            }
            offsets += curOffset
            curOffset += handleList.size().toLong
            numRows += 1
          }
        }
        offsets += curOffset

        val buffer0 = ByteBuffer.wrap(cdi0.toBytes)
        val buffer1 = ByteBuffer.wrap(cdi1.toBytes)

        val nullBitMaps = DataType.setAllNotNullBitMapWithNumRows(numRows)

        val regionIdType = IntegerType.BIGINT
        val handleListType = ArrayType.ARRAY

        val childColumnVectors = new ArrayBuffer[TiColumnVector]
        childColumnVectors +=
          new TiChunkColumnVector(
            regionIdType,
            regionIdType.getFixLen,
            numRows,
            0,
            nullBitMaps,
            null,
            buffer0)
        childColumnVectors +=
          // any type will do? actual type is array[Long]
          new TiChunkColumnVector(
            handleListType,
            8,
            curOffset.toInt,
            0,
            nullBitMaps,
            offsets.toArray,
            buffer1)
        val chunk = new TiChunk(childColumnVectors.toArray)
        TiColumnarBatchHelper.createColumnarBatch(chunk)
      }
    }.asInstanceOf[Iterator[InternalRow]]
}
