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

import com.pingcap.tikv.kvproto.Coprocessor.KeyRange
import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.SchemaInfer
import com.pingcap.tikv.operation.iterator.CoprocessIterator
import com.pingcap.tikv.operation.transformer.RowTransformer
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.util.{KeyRangeUtils, RangeSplitter}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.listener.CacheListenerManager
import com.pingcap.tispark.{TiDBRelation, TiSessionCache, TiUtils}
import gnu.trove.list.array
import gnu.trove.list.array.TLongArrayList
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, GenericInternalRow, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, Metadata}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable

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
    s"TiDB $nodeName{${tiRdd.dagRequest.toString}}" +
      s"${TiUtils.getReqEstCountStr(tiRdd.dagRequest)}"
  }

  override def simpleString: String = verboseString
}

/**
 * HandleRDDExec is used for scanning handles from TiKV as a LeafExecNode in index plan.
 * Providing handle scan via a TiHandleRDD.
 *
 * @param tiHandleRDD handle source
 */
case class HandleRDDExec(tiHandleRDD: TiHandleRDD) extends LeafExecNode {
  override val nodeName: String = "HandleRDD"

  override lazy val metrics = Map(
    "numOutputRegions" -> SQLMetrics.createMetric(sparkContext, "number of regions")
  )

  override val outputPartitioning: Partitioning = UnknownPartitioning(0)

  val internalRDD: RDD[InternalRow] =
    RDDConversions.rowToRowRdd(tiHandleRDD, output.map(_.dataType))

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRegions = longMetric("numOutputRegions")

    internalRDD.mapPartitionsWithIndexInternal { (index, iter) =>
      val proj = UnsafeProjection.create(schema)
      proj.initialize(index)
      iter.map { r =>
        numOutputRegions += 1
        proj(r)
      }
    }
  }

  final lazy val attributeRef = Seq(
    AttributeReference("RegionId", LongType, nullable = false, Metadata.empty)(),
    AttributeReference(
      "Handles",
      ArrayType(LongType, containsNull = false),
      nullable = false,
      Metadata.empty
    )()
  )

  override def output: Seq[Attribute] = attributeRef

  override def verboseString: String = {
    s"TiDB $nodeName{${tiHandleRDD.dagRequest.toString}}" +
      s"${TiUtils.getReqEstCountStr(tiHandleRDD.dagRequest)}"
  }

  override def simpleString: String = verboseString
}

/**
 * RegionTaskExec is used for issuing requests which are generated based on handles retrieved from
 * [[HandleRDDExec]] aggregated by a [[org.apache.spark.sql.execution.aggregate.SortAggregateExec]]
 * with [[org.apache.spark.sql.catalyst.expressions.aggregate.CollectHandles]] as aggregate function.
 *
 * RegionTaskExec will downgrade a index scan plan to table scan plan if handles retrieved from one
 * region exceed spark.tispark.plan.downgrade.index_threshold in your spark config.
 *
 * Refer to code in [[TiDBRelation]] and [[CoprocessorRDD]] for further details.
 *
 */
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
    "numDowngradedTasks" -> SQLMetrics.createMetric(sparkContext, "number of downgraded tasks"),
    "numIndexScanTasks" -> SQLMetrics
      .createMetric(sparkContext, "number of index double read tasks")
  )

  private val appId = SparkContext.getOrCreate().appName
  private val downgradeThreshold = session.getConf.getRegionIndexScanDowngradeThreshold

  type TiRow = com.pingcap.tikv.row.Row

  override val nodeName: String = "RegionTaskExec"
  // cache invalidation call back function
  // used for driver to update PD cache
  private val callBackFunc = CacheListenerManager.getInstance().CACHE_ACCUMULATOR_FUNCTION

  def rowToInternalRow(row: Row, outputTypes: Seq[DataType]): InternalRow = {
    val numColumns = outputTypes.length
    val mutableRow = new GenericInternalRow(numColumns)
    val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)
    var i = 0
    while (i < numColumns) {
      mutableRow(i) = converters(i)(row(i))
      i += 1
    }

    mutableRow
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numHandles = longMetric("numHandles")
    val numIndexScanTasks = longMetric("numIndexScanTasks")
    val numDowngradedTasks = longMetric("numDowngradedTasks")
    child
      .execute()
      .mapPartitionsWithIndexInternal { (index, iter) =>
        // For each partition, we do some initialization work
        val logger = Logger.getLogger(getClass.getName)
        logger.info(s"In partition No.$index")
        val session = TiSessionCache.getSession(appId, tiConf)
        session.injectCallBackFunc(callBackFunc)
        val batchSize = session.getConf.getIndexScanBatchSize
        // We need to clear index info in order to perform table scan
        dagRequest.clearIndexInfo()
        val schemaInferrer: SchemaInfer = SchemaInfer.create(dagRequest)
        val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
        val finalTypes = rowTransformer.getTypes.toList

        iter.flatMap { row =>
          val handles = row.getArray(1).toLongArray()
          val handleIterator: util.Iterator[Long] = handles.iterator
          var taskCount = 0

          val completionService =
            new ExecutorCompletionService[util.Iterator[TiRow]](session.getThreadPoolForIndexScan)
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

          /**
           * Checks whether the number of handles retrieved from TiKV exceeds the `downgradeThreshold`.
           *
           * @return true, the number of handles retrieved exceeds the `downgradeThreshold`, false otherwise.
           */
          def satisfyDowngradeThreshold: Boolean = {
            handles.length > downgradeThreshold
          }

          /**
           * Checks whether the tasks are valid.
           *
           * Currently we only check whether the task list contains only one [[RegionTask]],
           * since in each partition, handles received are from the same region.
           *
           * @param tasks tasks to examine
           */
          def proceedTasksOrThrow(tasks: Seq[RegionTask]): Unit = {
            if (tasks.lengthCompare(1) != 0) {
              throw new RuntimeException(s"Unexpected region task size:${tasks.size}, expecting 1")
            }
          }

          /**
           * If one task's ranges list exceeds some threshold, we split it into tow sub tasks and
           * each has half of the original ranges.
           *
           * @param tasks task list to examine
           * @return split task list
           */
          def splitTasks(tasks: Seq[RegionTask]): mutable.Seq[RegionTask] = {
            val finalTasks = mutable.ListBuffer[RegionTask]()
            tasks.foreach(finalTasks += _)
            while (finalTasks.exists(isTaskRangeSizeInvalid)) {
              val tasksToSplit = mutable.ListBuffer[RegionTask]()
              finalTasks.filter(isTaskRangeSizeInvalid).foreach(tasksToSplit.add)
              tasksToSplit.foreach(task => {
                val newRanges = task.getRanges.grouped(task.getRanges.size() / 2)
                newRanges.foreach(range => {
                  finalTasks += RegionTask.newInstance(task.getRegion, task.getStore, range)
                })
                finalTasks -= task
              })
            }
            logger.info(s"Split ${tasks.size} tasks into ${finalTasks.size} tasks.")
            finalTasks
          }

          def isTaskRangeSizeInvalid(task: RegionTask): Boolean = {
            task == null ||
            task.getRanges.size() > tiConf.getMaxRequestKeyRangeSize
          }

          def submitTasks(tasks: List[RegionTask]): Unit = {
            taskCount += 1
            val task = new Callable[util.Iterator[TiRow]] {
              override def call(): util.Iterator[TiRow] = {
                CoprocessIterator.getRowIterator(dagRequest, tasks, session)
              }
            }
            completionService.submit(task)
          }

          def doIndexScan(): Unit = {
            while (handleIterator.hasNext) {
              val handleList = feedBatch()
              numHandles += handleList.size()
              logger.info("Single batch handles size:" + handleList.size())
              numIndexScanTasks += 1
              var tasks = RangeSplitter
                .newSplitter(session.getRegionManager)
                .splitHandlesByRegion(
                  dagRequest.getTableInfo.getId,
                  handleList
                )
              proceedTasksOrThrow(tasks)
              tasks = splitTasks(tasks)

              logger.info(s"Single batch RegionTask size:${tasks.size()}")
              tasks.foreach(task => {
                logger.info(
                  s"Single batch RegionTask={Host:${task.getHost}," +
                    s"Region:${task.getRegion}," +
                    s"Store:{id=${task.getStore.getId},address=${task.getStore.getAddress}}, " +
                    s"RangesListSize:${task.getRanges.size()}}"
                )
              })

              submitTasks(tasks.toList)
            }
          }

          /**
           * We merge potentially discrete index ranges from `taskRanges` into one large range
           * and create a new [[RegionTask]] for the downgraded plan to execute. Should add
           * filters for DAG request.
           *
           * @param taskRanges the index scan ranges
           */
          def doDowngradeScan(taskRanges: List[KeyRange]): Unit = {
            // Restore original filters to perform downgraded table scan logic
            dagRequest.resetFilters(dagRequest.getDowngradeFilters)

            val tasks = RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitRangeByRegion(KeyRangeUtils.mergeRanges(taskRanges))
            proceedTasksOrThrow(tasks)

            val task = tasks.head
            logger.info(
              s"Merged ${taskRanges.size()} index ranges to ${task.getRanges.size()} ranges."
            )
            logger.info(
              s"Unary task downgraded, task info:Host={${task.getHost}}, " +
                s"RegionId={${task.getRegion.getId}}, " +
                s"Store={id=${task.getStore.getId},addr=${task.getStore.getAddress}}, " +
                s"RangesListSize=${task.getRanges.size()}}"
            )
            numDowngradedTasks += 1

            submitTasks(tasks.toList)
          }

          def checkIsUnaryRange(regionTask: RegionTask): Boolean = {
            regionTask.getRanges.size() == 1
          }

          if (satisfyDowngradeThreshold) {
            val handleList = new TLongArrayList()
            handles.foreach { handleList.add }
            // After `splitHandlesByRegion`, ranges in the task are arranged in order
            val tasks = RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitHandlesByRegion(
                dagRequest.getTableInfo.getId,
                handleList
              )
            proceedTasksOrThrow(tasks)

            // if the original index scan task contains only one KeyRange, we don't need to downgrade that since downgraded
            // plan will result in more filters and may be slower than the original index scan plan.
            if (checkIsUnaryRange(tasks.head)) {
              logger.info(s"Unary index scan range found, performing index scan.")
              doIndexScan()
            } else {
              // Should downgrade to full table scan for one region
              val taskRanges = tasks.head.getRanges
              logger.warn(
                s"Index scan handle size:${handles.length} exceed downgrade threshold:$downgradeThreshold" +
                  s", downgrade to table scan with ${tasks.size()} region tasks, " +
                  s"original index scan task has ${taskRanges.size()} ranges, will try to merge."
              )
              doDowngradeScan(taskRanges.toList)
            }
          } else {
            // Request doesn't need to be downgraded
            doIndexScan()
          }

          // The result iterator serves as an wrapper to the final result we fetched from region tasks
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

              // RowIterator has not been initialized
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
              numOutputRows += 1
              // Unsafe row projection
              val proj = UnsafeProjection.create(schema)
              proj.initialize(index)
              val sparkRow = toSparkRow(rowIterator.next())
              val outputTypes = output.map(_.dataType)
              // Need to convert spark row to internal row for Catalyst
              proj(rowToInternalRow(sparkRow, outputTypes))
            }
          }
          resultIter
        }
      }
  }

  override def verboseString: String = {
    s"TiSpark $nodeName{downgradeThreshold=$downgradeThreshold,downgradeFilter=${dagRequest.getFilters}"
  }

  override def simpleString: String = verboseString
}
