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

package org.apache.spark.sql.execution

import java.util.logging.Logger

import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.util.{KeyRangeUtils, RangeSplitter}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.TiSessionCache
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.exchange.ShuffleExchange
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.{LongType, Metadata}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._

case class CoprocessorRDD(output: Seq[Attribute], tiRdd: TiRDD) extends LeafExecNode {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  )

  override val nodeName: String = "CoprocessorRDD"
  override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  override val outputOrdering: Seq[SortOrder] = Nil

  val internalRdd: RDD[InternalRow] = RDDConversions.rowToRowRdd(tiRdd, output.map(_.dataType))

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    internalRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  override def verboseString: String = {
    s"TiDB $nodeName{${tiRdd.dagRequest.toString}}"
  }

  override def simpleString: String = verboseString
}

case class HandleRDDExec(tiHandleRDD: TiHandleRDD) extends LeafExecNode {
  override val nodeName: String = "HandleRDD"

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of selected handles")
  )

  override val outputPartitioning: Partitioning = UnknownPartitioning(0)

  val internalRDD: RDD[InternalRow] =
    RDDConversions.rowToRowRdd(tiHandleRDD, output.map(_.dataType))

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    internalRDD.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        proj(r)
      }
    }
  }

  final lazy val attributeRef = Seq(
    AttributeReference("RegionId", LongType, nullable = false, Metadata.empty)(),
    AttributeReference("Handle", LongType, nullable = false, Metadata.empty)()
  )

  override def output: Seq[Attribute] = attributeRef

  override def verboseString: String = {
    s"TiDB $nodeName{${tiHandleRDD.dagRequest.toString}}"
  }

  override def simpleString: String = verboseString
}

case class RegionTaskExec(child: SparkPlan,
                          output: Seq[Attribute],
                          dagRequest: TiDAGRequest,
                          tiConf: TiConfiguration,
                          ts: TiTimestamp,
                          @transient private val session: TiSession,
                          @transient private val sparkSession: SparkSession)
    extends UnaryExecNode {

  private val logger = Logger.getLogger(getClass.getName)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numHandles" -> SQLMetrics.createMetric(sparkContext, "number of collected handles"),
    "numRegionTasks" -> SQLMetrics.createMetric(sparkContext, "number of executed region tasks")
  )

  private val appId = SparkContext.getOrCreate().appName

  type TiRow = com.pingcap.tikv.row.Row

  override val nodeName: String = "RegionTaskExec"

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numHandles = longMetric("numHandles")
    val numRegionTasks = longMetric("numRegionTasks")
    child
      .execute()
      .mapPartitionsWithIndexInternal { (index, iter) =>
        iter.flatMap { row =>
          val handles = row.getArray(1).toLongArray()
          val session = TiSessionCache.getSession(appId, tiConf)
          val handleList = new TLongArrayList()
          handles.foreach(handleList.add)
          dagRequest.clearIndexInfo()

          val keyWithRegionTasks = RangeSplitter
            .newSplitter(session.getRegionManager)
            .splitHandlesByRegion(
              dagRequest.getTableInfo.getId,
              handleList
            )
          numHandles += handles.length
          logger.info("Handle.size():" + handles.length)
          numRegionTasks += keyWithRegionTasks.size()
          logger.info("KeyWithRegionTasks.size():" + keyWithRegionTasks.size())
          val firstTask = keyWithRegionTasks.head
          logger.info(
            s"RegionTask=>Host:${firstTask.getHost},RegionId:${firstTask.getRegion.getId},Store:{id=${firstTask.getStore.getId},addr=${firstTask.getStore.getAddress}}"
          )
          val snapshot = session.createSnapshot(ts)
          val iterator =
            snapshot.tableRead(dagRequest, keyWithRegionTasks)
          val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
          val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
          val finalTypes = rowTransformer.getTypes.toList

          def toSparkRow(row: TiRow): Row = {
            val transRow = rowTransformer.transform(row)
            val rowArray = new Array[Any](finalTypes.size)

            for (i <- 0 until transRow.fieldCount) {
              rowArray(i) = transRow.get(i, finalTypes(i))
            }

            Row.fromSeq(rowArray)
          }

          new Iterator[UnsafeRow] {
            override def hasNext: Boolean = iterator.hasNext

            override def next(): UnsafeRow = {
              val proj = UnsafeProjection.create(schema)
              proj.initialize(index)
              val sparkRow = toSparkRow(iterator.next())
              numOutputRows += 1
              proj(InternalRow.fromSeq(sparkRow.toSeq))
            }
          }
        }
      }
  }

  override def verboseString: String = {
    s"TiSpark $nodeName{range=${dagRequest.getRanges.map(KeyRangeUtils.toString)}}"
  }

  override def simpleString: String = verboseString
}
