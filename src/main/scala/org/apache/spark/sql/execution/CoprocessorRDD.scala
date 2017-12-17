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

import java.util
import java.util.concurrent.{Callable, ExecutorCompletionService}

import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.iterator.CoprocessIterator
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.util.{KeyRangeUtils, RangeSplitter}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.{TiConfigConst, TiPartition, TiSessionCache}
import gnu.trove.list.array
import gnu.trove.list.array.TLongArrayList
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.{LongType, Metadata}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of selected handles"),
    "handleReadTime" -> SQLMetrics.createMetric(sparkContext, "time of handles selection in ms")
  )

  override val outputPartitioning: Partitioning = UnknownPartitioning(0)

  val internalRDD: RDD[InternalRow] =
    RDDConversions.rowToRowRdd(tiHandleRDD, output.map(_.dataType))

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val handleReadTime = longMetric("handleReadTime")

    internalRDD.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map { r =>
        handleReadTime += tiHandleRDD.lastHandelReadTimeMs
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

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "numHandles" -> SQLMetrics.createMetric(sparkContext, "number of handles used in double scan"),
    "numRegionTasks" -> SQLMetrics.createMetric(sparkContext, "number of executed region tasks")
  )

  private val appId = SparkContext.getOrCreate().appName
  private val downgradeThreshold = session.getConf.getRegionIndexScanDowngradeThreshold

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
          val logger = Logger.getLogger(getClass.getName)
          val handles = row.getArray(1).toLongArray()
          val session = TiSessionCache.getSession(appId, tiConf)
          val handleIterator: util.Iterator[Long] = handles.iterator
          var taskCount = 0
          val batchSize = session.getConf.getIndexScanBatchSize

          val completionService =
            new ExecutorCompletionService[util.Iterator[TiRow]](session.getThreadPoolForIndexScan)
          val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
          val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
          val finalTypes = rowTransformer.getTypes.toList
          var rowIterator: util.Iterator[TiRow] = null

          def feedBatch(): TLongArrayList = {
            val handles = new array.TLongArrayList(512)
            while (handleIterator.hasNext &&
                   handles.size() < batchSize) {
              handles.add(handleIterator.next())
            }
            handles
          }

          def toSparkRow(row: TiRow): Row = {
            val transRow = rowTransformer.transform(row)
            val rowArray = new Array[Any](finalTypes.size)

            for (i <- 0 until transRow.fieldCount) {
              rowArray(i) = transRow.get(i, finalTypes(i))
            }

            Row.fromSeq(rowArray)
          }

          def isDowngrade: Boolean = {
            handles.length > downgradeThreshold
          }

          def proceedTasksOrThrow(tasks: Seq[RegionTask]): Unit = {
            if (tasks.lengthCompare(1) != 0) {
              throw new RuntimeException(s"Unexpected region task size:${tasks.size}, expecting 1")
            }
          }

          // We need to clear index info in order to perform table scan
          dagRequest.clearIndexInfo()

          // Downgrade to full table scan for a region
          if (isDowngrade) {
            val handleList = new TLongArrayList()
            handles.foreach(handleList.add)
            dagRequest.resetFilters(dagRequest.getDowngradeFilters)

            var tasks = RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitHandlesByRegion(
                dagRequest.getTableInfo.getId,
                handleList
              )
            proceedTasksOrThrow(tasks)

            logger.warn(
              s"Index scan handle size:${handles.length} exceed downgrade threshold:$downgradeThreshold" +
                s", downgrade to table scan with ${tasks.size()} region tasks"
            )

            tasks = RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitRangeByRegion(tasks.head.getRanges)
            proceedTasksOrThrow(tasks)

            val task = tasks.head
            logger.info(
              s"Unary task downgraded, task info:Host={${task.getHost}}, " +
                s"Region={${task.getRegion}}, " +
                s"Store={id=${task.getStore.getId},addr=${task.getStore.getAddress}}"
            )
            val hostTasksMap = new mutable.HashMap[String, mutable.Set[RegionTask]]
            with mutable.MultiMap[String, RegionTask]

            //TODO : some split configs
            var index = 0
            val result = new ListBuffer[Seq[RegionTask]]
            tasks.foreach(task => {
              hostTasksMap.addBinding(task.getHost, task)
              val tasks = hostTasksMap(task.getHost)
              if (tasks.nonEmpty) {
                result.append(tasks.toSeq)
                index += 1
                hostTasksMap.remove(task.getHost)
              }
            })
            // add rest
            for (tasks <- hostTasksMap.values) {
              result.append(tasks.toSeq)
              index += 1
            }
            numRegionTasks += result.size

            result.foreach(taskList => {
              taskCount += 1
              val task = new Callable[util.Iterator[TiRow]] {
                override def call(): util.Iterator[TiRow] = {
                  CoprocessIterator.getRowIterator(dagRequest, taskList, session)
                }
              }
              // TODO: may need to use other completionService rather than index scan's
              // Still, is okay to do like this.
              completionService.submit(task)
            })
          } else {
            while (handleIterator.hasNext) {
              val handleList = feedBatch()
              taskCount += 1
              val task = new Callable[util.Iterator[TiRow]] {
                override def call(): util.Iterator[TiRow] = {
                  val tasks = RangeSplitter
                    .newSplitter(session.getRegionManager)
                    .splitHandlesByRegion(
                      dagRequest.getTableInfo.getId,
                      handleList
                    )
                  proceedTasksOrThrow(tasks)

                  numHandles += handleList.size()
                  logger.info("Single batch handles size:" + handleList.size())
                  numRegionTasks += tasks.size()
                  logger.info("Single batch RegionTasks size:" + tasks.size())
                  val firstTask = tasks.head
                  logger.info(
                    s"Single batch first RegionTask={Host:${firstTask.getHost}," +
                      s"Region:${firstTask.getRegion}," +
                      s"Store:{id=${firstTask.getStore.getId},addr=${firstTask.getStore.getAddress}}}"
                  )

                  CoprocessIterator.getRowIterator(dagRequest, tasks, session)
                }
              }
              completionService.submit(task)
            }
          }

          val resultIter = new util.Iterator[UnsafeRow] {
            override def hasNext: Boolean = {

              def proceedNextBatchTask(): Boolean = {
                // For each batch fetch job, we get the first rowIterator with row data
                while (taskCount > 0) {
                  rowIterator = completionService.take().get()
                  taskCount -= 1

                  // If current rowIterator has any data, return true
                  if (rowIterator.hasNext) {
                    return true
                  }
                }
                // No rowIterator in any remaining batch fetch jobs contains data, return false
                false
              }

              // RowIter has not been initialized
              if (rowIterator == null) {
                proceedNextBatchTask()
              } else {
                if (rowIterator.hasNext) {
                  return true
                }
                proceedNextBatchTask()
              }
            }

            override def next(): UnsafeRow = {
              // Unsafe row projection
              val proj = UnsafeProjection.create(schema)
              proj.initialize(index)
              val sparkRow = toSparkRow(rowIterator.next())
              numOutputRows += 1
              proj(InternalRow.fromSeq(sparkRow.toSeq))
            }
          }
          resultIter
        }
      }
  }

  override def verboseString: String = {
    s"TiSpark $nodeName{downgradeThreshold=$downgradeThreshold}"
  }

  override def simpleString: String = verboseString
}
