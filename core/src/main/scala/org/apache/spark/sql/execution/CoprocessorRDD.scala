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
import com.pingcap.tispark.{TiConfigConst, TiDBRelation, TiSessionCache, TiUtils}
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
  private lazy val project = UnsafeProjection.create(schema)

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    internalRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      project.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        project(r)
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
  private lazy val project = UnsafeProjection.create(schema)

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRegions = longMetric("numOutputRegions")

    internalRDD.mapPartitionsWithIndexInternal { (index, iter) =>
      project.initialize(index)
      iter.map { r =>
        numOutputRegions += 1
        project(r)
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
      .createMetric(sparkContext, "number of index double read tasks"),
    "numRegions" -> SQLMetrics.createMetric(sparkContext, "number of regions"),
    "numIndexRangesScanned" -> SQLMetrics
      .createMetric(sparkContext, "number of index ranges scanned"),
    "numDowngradeRangesScanned" -> SQLMetrics
      .createMetric(sparkContext, "number of downgrade ranges scanned")
  )

  private val sqlConf = sqlContext.conf
  private val appId = SparkContext.getOrCreate().appName
  private val downgradeThreshold =
    sqlConf.getConfString(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD, "10000").toInt
  private lazy val project = UnsafeProjection.create(schema)

  type TiRow = com.pingcap.tikv.row.Row

  override val nodeName: String = "RegionTaskExec"

  def rowToInternalRow(row: Row,
                       outputTypes: Seq[DataType],
                       converters: Seq[Any => Any]): InternalRow = {
    val mutableRow = new GenericInternalRow(outputTypes.length)
    for (i <- outputTypes.indices) {
      mutableRow(i) = converters(i)(row(i))
    }

    mutableRow
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numHandles = longMetric("numHandles")
    val numIndexScanTasks = longMetric("numIndexScanTasks")
    val numDowngradedTasks = longMetric("numDowngradedTasks")
    val numRegions = longMetric("numRegions")
    val numIndexRangesScanned = longMetric("numIndexRangesScanned")
    val numDowngradeRangesScanned = longMetric("numDowngradeRangesScanned")

    val downgradeSplitFactor = 4

    val downgradeDagRequest = dagRequest.copy()
    // We need to clear index info in order to perform table scan
    downgradeDagRequest.clearIndexInfo()
    downgradeDagRequest.resetFilters(downgradeDagRequest.getDowngradeFilters)

    child
      .execute()
      .mapPartitionsWithIndexInternal { (index, iter) =>
        // For each partition, we do some initialization work
        val logger = Logger.getLogger(getClass.getName)
        logger.info(s"In partition No.$index")
        val session = TiSessionCache.getSession(appId, tiConf)
        val batchSize = tiConf.getIndexScanBatchSize

        iter.flatMap { row =>
          val handles = row.getArray(1).toLongArray()
          val handleIterator: util.Iterator[Long] = handles.iterator
          var taskCount = 0
          numRegions += 1

          val completionService =
            new ExecutorCompletionService[util.Iterator[TiRow]](session.getThreadPoolForIndexScan)
          var rowIterator: util.Iterator[TiRow] = null

          /**
           * Checks whether the tasks are valid.
           *
           * Currently we only check whether the task list contains only one [[RegionTask]],
           * since in each partition, handles received are from the same region.
           *
           * @param tasks tasks to examine
           */
          def proceedTasksOrThrow(tasks: Seq[RegionTask]): Seq[RegionTask] = {
            if (tasks.lengthCompare(1) != 0) {
              throw new RuntimeException(s"Unexpected region task size:${tasks.size}, expecting 1")
            }
            tasks
          }

          // After `splitAndSortHandlesByRegion`, ranges in the task are arranged in order
          // TODO: Maybe we can optimize splitAndSortHandlesByRegion if we are sure the handles are in same region?
          val indexTasks = proceedTasksOrThrow(
            RangeSplitter
              .newSplitter(session.getRegionManager)
              .splitAndSortHandlesByRegion(
                dagRequest.getTableInfo.getId,
                new TLongArrayList(handles)
              )
          )
          val indexTaskRanges = indexTasks.head.getRanges

          def feedBatch(): TLongArrayList = {
            val handles = new array.TLongArrayList(512)
            while (handleIterator.hasNext &&
                   handles.size() < batchSize) {
              handles.add(handleIterator.next())
            }
            handles
          }

          /**
           * Checks whether the number of handle ranges retrieved from TiKV exceeds the `downgradeThreshold` after handle merge.
           *
           * @return true, the number of handle ranges retrieved exceeds the `downgradeThreshold` after handle merge, false otherwise.
           */
          def satisfyDowngradeThreshold: Boolean = {
            indexTaskRanges.size() > downgradeThreshold
          }

          /**
           * If one task's ranges list exceeds some threshold, we split it into two sub tasks and
           * each has half of the original ranges.
           *
           * @param tasks task list to examine
           * @return split task list
           */
          def splitTasks(tasks: Seq[RegionTask]): mutable.Seq[RegionTask] = {
            val finalTasks = mutable.ListBuffer[RegionTask]()
            val queue = mutable.Queue[RegionTask]()
            for (task <- tasks) {
              queue += task
              while (queue.nonEmpty) {
                val front = queue.dequeue
                if (isTaskRangeSizeInvalid(front)) {
                  // use (size + 1) / 2 here rather than size / 2
                  // to avoid extra single task generated by odd list
                  front.getRanges
                    .grouped((task.getRanges.size() + 1) / 2)
                    .foreach(range => {
                      queue += RegionTask.newInstance(task.getRegion, task.getStore, range)
                    })
                } else {
                  // add all ranges satisfying task range size to final task list
                  finalTasks += front
                }
              }
            }
            logger.info(s"Split ${tasks.size} tasks into ${finalTasks.size} tasks.")
            finalTasks
          }

          def isTaskRangeSizeInvalid(task: RegionTask): Boolean = {
            task == null ||
            task.getRanges.size() > tiConf.getMaxRequestKeyRangeSize
          }

          def submitTasks(tasks: List[RegionTask], dagRequest: TiDAGRequest): Unit = {
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
              // After `splitAndSortHandlesByRegion`, ranges in the task are arranged in order
              // TODO: Maybe we can optimize splitAndSortHandlesByRegion if we are sure the handles are in same region?
              val task = proceedTasksOrThrow(
                RangeSplitter
                  .newSplitter(session.getRegionManager)
                  .splitAndSortHandlesByRegion(
                    dagRequest.getTableInfo.getId,
                    new TLongArrayList(handles)
                  )
              )
              val taskRange = task.head.getRanges
              val tasks = splitTasks(task)
              numIndexScanTasks += tasks.size

              logger.info(s"Single batch RegionTask size:${tasks.size}")
              tasks.foreach(task => {
                logger.info(
                  s"Single batch RegionTask={Host:${task.getHost}," +
                    s"Region:${task.getRegion}," +
                    s"Store:{id=${task.getStore.getId},address=${task.getStore.getAddress}}, " +
                    s"RangesListSize:${task.getRanges.size}}"
                )
              })

              submitTasks(tasks.toList, dagRequest)
              numIndexRangesScanned += taskRange.size
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
            // TODO: Maybe we can optimize splitRangeByRegion if we are sure the key ranges are in the same region?
            val downgradeTasks = proceedTasksOrThrow(
              try {
                RangeSplitter
                  .newSplitter(session.getRegionManager)
                  .splitSortedRangeInRegion(taskRanges, downgradeSplitFactor)
              } catch {
                case e: Exception =>
                  logger.warn("Encountered problems when splitting range for single region.")
                  logger.warn("Retrying split with unified logic")
                  logger.warn("Exception message: " + e.getMessage)
                  RangeSplitter
                    .newSplitter(session.getRegionManager)
                    .splitRangeByRegion(KeyRangeUtils.mergeSortedRanges(taskRanges))
              }
            )

            val task = downgradeTasks.head
            val downgradeTaskRanges = task.getRanges
            logger.info(
              s"Merged ${taskRanges.size} index ranges to ${downgradeTaskRanges.size} ranges."
            )
            logger.info(
              s"Unary task downgraded, task info:Host={${task.getHost}}, " +
                s"RegionId={${task.getRegion.getId}}, " +
                s"Store={id=${task.getStore.getId},addr=${task.getStore.getAddress}}, " +
                s"RangesListSize=${downgradeTaskRanges.size}}"
            )
            numDowngradedTasks += 1
            numDowngradeRangesScanned += downgradeTaskRanges.size

            submitTasks(downgradeTasks.toList, downgradeDagRequest)
          }

          val schemaInferrer: SchemaInfer = if (satisfyDowngradeThreshold) {
            // Should downgrade to full table scan for one region
            logger.warn(
              s"Index scan task range size = ${indexTaskRanges.size}, " +
                s"exceeding downgrade threshold = $downgradeThreshold, " +
                s"index scan handle size = ${handles.length}, will try to merge."
            )
            doDowngradeScan(indexTaskRanges.toList)
            SchemaInfer.create(downgradeDagRequest)
          } else {
            // Request doesn't need to be downgraded
            doIndexScan()
            SchemaInfer.create(dagRequest)
          }

          val rowTransformer: RowTransformer = schemaInferrer.getRowTransformer
          val finalTypes = rowTransformer.getTypes.toList
          val outputTypes = output.map(_.dataType)
          val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)

          def toSparkRow(row: TiRow): Row = {
            val transRow = rowTransformer.transform(row)
            val rowArray = new Array[Any](finalTypes.size)

            for (i <- 0 until transRow.fieldCount) {
              rowArray(i) = transRow.get(i, finalTypes(i))
            }

            Row.fromSeq(rowArray)
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
              project.initialize(index)
              val sparkRow = toSparkRow(rowIterator.next())
              // Need to convert spark row to internal row for Catalyst
              project(rowToInternalRow(sparkRow, outputTypes, converters))
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
