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
import com.pingcap.tispark.listener.CacheInvalidateListener
import com.pingcap.tispark.utils.ReflectionUtil._
import com.pingcap.tispark.utils.{TiConverter, TiUtil}
import gnu.trove.list.array
import gnu.trove.list.array.TLongArrayList
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow, SortOrder, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.tispark.{TiHandleRDD, TiRDD}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, Metadata}
import org.apache.spark.sql.{Row, SparkSession}
import org.tikv.kvproto.Coprocessor.KeyRange

import scala.collection.JavaConversions._
import scala.collection.mutable

case class CoprocessorRDD(output: Seq[Attribute], tiRdds: List[TiRDD]) extends LeafExecNode {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows")
  )

  override val nodeName: String = "CoprocessorRDD"
  override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  override val outputOrdering: Seq[SortOrder] = Nil

  private val internalRDDs: List[RDD[InternalRow]] =
    tiRdds.map(rdd => RDDConversions.rowToRowRdd(rdd, output.map(_.dataType)))
  private lazy val project = UnsafeProjection.create(schema)

  private def internalRowToUnsafeRowWithIndex(
    numOutputRows: SQLMetric
  ): (Int, Iterator[InternalRow]) => Iterator[UnsafeRow] =
    (index, iter) => {
      project.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        project(r)
      }
    }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    internalRDDs
      .map(
        rdd =>
          ReflectionMapPartitionWithIndexInternal(
            rdd,
            internalRowToUnsafeRowWithIndex(numOutputRows)
          ).invoke()
      )
      .reduce(_ union _)

  }

  def dagRequest: TiDAGRequest = tiRdds.head.dagRequest

  override def verboseString: String =
    if (tiRdds.size > 1) {
      val b = new StringBuilder
      b.append(s"TiSpark $nodeName on partition table:\n")
      tiRdds.zipWithIndex.map {
        case (_, i) => b.append(s"partition p$i")
      }
      b.append(s"with dag request: $dagRequest")
      b.toString()
    } else {
      s"TiSpark $nodeName{$dagRequest}" +
        s"${TiUtil.getReqEstCountStr(dagRequest)}"

    }

  override def simpleString: String = verboseString
}

/**
 * HandleRDDExec is used for scanning handles from TiKV as a LeafExecNode in index plan.
 * Providing handle scan via a TiHandleRDD.
 *
 * @param tiHandleRDDs handle source
 */
case class HandleRDDExec(tiHandleRDDs: List[TiHandleRDD]) extends LeafExecNode {
  override val nodeName: String = "HandleRDD"

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "numOutputRegions" -> SQLMetrics.createMetric(sparkContext, "number of regions")
  )

  override val outputPartitioning: Partitioning = UnknownPartitioning(0)

  private val internalRDDs: List[RDD[InternalRow]] =
    tiHandleRDDs.map(rdd => RDDConversions.rowToRowRdd(rdd, output.map(_.dataType)))
  private lazy val project = UnsafeProjection.create(schema)

  private def internalRowToUnsafeRowWithIndex(
    numOutputRegions: SQLMetric
  ): (Int, Iterator[InternalRow]) => Iterator[UnsafeRow] =
    (index, iter) => {
      project.initialize(index)
      iter.map { r =>
        numOutputRegions += 1
        project(r)
      }
    }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRegions = longMetric("numOutputRegions")

    internalRDDs
      .map(
        rdd =>
          ReflectionMapPartitionWithIndexInternal(
            rdd,
            internalRowToUnsafeRowWithIndex(numOutputRegions)
          ).invoke()
      )
      .reduce(_ union _)
  }

  final lazy val attributeRef = Seq(
    newAttributeReference("RegionId", LongType, nullable = false, Metadata.empty),
    newAttributeReference(
      "Handles",
      ArrayType(LongType, containsNull = false),
      nullable = false,
      Metadata.empty
    )
  )

  override def output: Seq[Attribute] = attributeRef

  def dagRequest: TiDAGRequest = tiHandleRDDs.head.dagRequest

  override def verboseString: String =
    if (tiHandleRDDs.size > 1) {
      val b = new mutable.StringBuilder()
      b.append(s"TiSpark $nodeName on partition table:\n")
      tiHandleRDDs.zipWithIndex.map {
        case (_, i) => b.append(s"partition p$i")
      }
      b.append(s"with dag request: $dagRequest")
      b.toString()
    } else {
      s"TiDB $nodeName{$dagRequest}" +
        s"${TiUtil.getReqEstCountStr(dagRequest)}"
    }

  override def simpleString: String = verboseString
}

/**
 * RegionTaskExec is used for issuing requests which are generated based
 * on handles retrieved from [[HandleRDDExec]] aggregated by a SortAggregateExec with
 * [[org.apache.spark.sql.catalyst.expressions.aggregate.CollectHandles]] as aggregate function.
 *
 * RegionTaskExec will downgrade a index scan plan to table scan plan if handles retrieved from one
 * region exceed spark.tispark.plan.downgrade.index_threshold in your spark config.
 *
 * Refer to code in [[com.pingcap.tispark.TiDBRelation]] and [[CoprocessorRDD]] for further details.
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

  override lazy val metrics: Map[String, SQLMetric] = Map(
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

  private val downgradeThreshold = 1000000000
  // FIXME: https://github.com/pingcap/tispark/issues/731
  // enable downgrade sqlConf.getConfString(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD, "10000").toInt
  private lazy val project = UnsafeProjection.create(schema)

  type TiRow = com.pingcap.tikv.row.Row

  override val nodeName: String = "RegionTaskExec"
  // cache invalidation call back function
  // used for driver to update PD cache
  private val callBackFunc = CacheInvalidateListener.getInstance()

  private def rowToInternalRow(row: Row,
                               outputTypes: Seq[DataType],
                               converters: Seq[Any => Any]): InternalRow = {
    val mutableRow = new GenericInternalRow(outputTypes.length)
    for (i <- outputTypes.indices) {
      mutableRow(i) = converters(i)(row(i))
    }

    mutableRow
  }

  private def isTaskRangeSizeInvalid(task: RegionTask): Boolean =
    task == null ||
      task.getRanges.size() > tiConf.getMaxRequestKeyRangeSize

  private def internalRowToUnsafeRowWithIndex(
    numOutputRows: SQLMetric,
    numHandles: SQLMetric,
    numIndexScanTasks: SQLMetric,
    numDowngradedTasks: SQLMetric,
    numRegions: SQLMetric,
    numIndexRangesScanned: SQLMetric,
    numDowngradeRangesScanned: SQLMetric,
    downgradeDagRequest: TiDAGRequest
  ): (Int, Iterator[InternalRow]) => Iterator[UnsafeRow] = { (index, iter) =>
    // For each partition, we do some initialization work
    val logger = Logger.getLogger(getClass.getName)
    logger.debug(s"In partition No.$index")
    val session = TiSession.getInstance(tiConf)
    session.injectCallBackFunc(callBackFunc)
    val batchSize = tiConf.getIndexScanBatchSize

    iter.flatMap { row =>
      val handles = row.getArray(1).toLongArray()
      val handleIterator: util.Iterator[Long] = handles.iterator
      var taskCount = 0
      numRegions += 1

      val completionService =
        new ExecutorCompletionService[util.Iterator[TiRow]](session.getThreadPoolForIndexScan)
      var rowIterator: util.Iterator[TiRow] = null

      // After `splitAndSortHandlesByRegion`, ranges in the task are arranged in order
      def generateIndexTasks(handles: TLongArrayList): util.List[RegionTask] = {
        val indexTasks: util.List[RegionTask] = new util.ArrayList[RegionTask]()
        indexTasks.addAll(
          RangeSplitter
            .newSplitter(session.getRegionManager)
            .splitAndSortHandlesByRegion(dagRequest.getIds, new TLongArrayList(handles))
        )
        indexTasks
      }

      // indexTasks was made to be used later to determine whether we should downgrade to
      // table scan or not.
      val indexTasks: util.List[RegionTask] = generateIndexTasks(new TLongArrayList(handles))
      val indexTaskRanges = indexTasks.flatMap {
        _.getRanges
      }

      // Checks whether the number of handle ranges retrieved from TiKV
      // exceeds the `downgradeThreshold` after handle merge.
      // returns true if the number of handle ranges retrieved exceeds
      // the `downgradeThreshold` after handle merge, false otherwise.
      def satisfyDowngradeThreshold: Boolean =
        indexTaskRanges.size() > downgradeThreshold

      def submitTasks(tasks: List[RegionTask], dagRequest: TiDAGRequest): Unit = {
        taskCount += 1
        val task = new Callable[util.Iterator[TiRow]] {
          override def call(): util.Iterator[TiRow] =
            CoprocessIterator.getRowIterator(dagRequest, tasks, session)
        }
        completionService.submit(task)
      }

      // If one task's ranges list exceeds some threshold, we split it into two sub tasks and
      // each has half of the original ranges.
      def splitTasks(task: RegionTask): mutable.Seq[RegionTask] = {
        val finalTasks = mutable.ListBuffer[RegionTask]()
        val queue = mutable.Queue[RegionTask]()
        queue += task
        while (queue.nonEmpty) {
          val front = queue.dequeue
          if (isTaskRangeSizeInvalid(front)) {
            // use (size + 1) / 2 here rather than size / 2
            // to avoid extra single task generated by odd list
            front.getRanges
              .grouped((front.getRanges.size() + 1) / 2)
              .foreach(range => {
                queue += RegionTask.newInstance(front.getRegion, front.getStore, range)
              })
          } else {
            // add all ranges satisfying task range size to final task list
            finalTasks += front
          }
        }
        logger.debug(s"Split $task into ${finalTasks.size} tasks.")
        finalTasks
      }

      def feedBatch(): TLongArrayList = {
        val handles = new array.TLongArrayList(512)
        while (handleIterator.hasNext &&
               handles.size() < batchSize) {
          handles.add(handleIterator.next())
        }
        handles
      }

      def doIndexScan(): Unit =
        while (handleIterator.hasNext) {
          val handleList: TLongArrayList = feedBatch()
          numHandles += handleList.size()
          logger.debug("Single batch handles size:" + handleList.size())

          val indexTasks: util.List[RegionTask] = generateIndexTasks(handleList)

          indexTasks.foreach { task =>
            val taskRange = task.getRanges
            val tasks = splitTasks(task)
            numIndexScanTasks += tasks.size

            if (logger.isDebugEnabled) {
              logger.debug(s"Single batch RegionTask size:${tasks.size}")
              tasks.foreach(task => {
                logger.debug(
                  s"Single batch RegionTask={Host:${task.getHost}," +
                    s"Region:${task.getRegion}," +
                    s"Store:{id=${task.getStore.getId},address=${task.getStore.getAddress}}, " +
                    s"RangesListSize:${task.getRanges.size}}"
                )
              })
            }

            submitTasks(tasks.toList, dagRequest)
            numIndexRangesScanned += taskRange.size
          }
        }

      // We merge potentially discrete index ranges from `taskRanges` into one large range
      // and create a new RegionTask for the downgraded plan to execute. Should add
      // filters for DAG request.
      // taskRanges defines the index scan ranges
      def doDowngradeScan(taskRanges: List[KeyRange]): Unit = {
        // Restore original filters to perform downgraded table scan logic
        // TODO: Maybe we can optimize splitRangeByRegion if we are sure the key ranges are in the same region?
        val downgradeTasks = RangeSplitter
          .newSplitter(session.getRegionManager)
          .splitRangeByRegion(KeyRangeUtils.mergeSortedRanges(taskRanges))

        downgradeTasks.foreach { task =>
          val downgradeTaskRanges = task.getRanges
          if (logger.isDebugEnabled) {
            logger.debug(
              s"Merged ${taskRanges.size} index ranges to ${downgradeTaskRanges.size} ranges."
            )
            logger.debug(
              s"Unary task downgraded, task info:Host={${task.getHost}}, " +
                s"RegionId={${task.getRegion.getId}}, " +
                s"Store={id=${task.getStore.getId},addr=${task.getStore.getAddress}}, " +
                s"RangesListSize=${downgradeTaskRanges.size}}"
            )
          }
          numDowngradedTasks += 1
          numDowngradeRangesScanned += downgradeTaskRanges.size

          submitTasks(downgradeTasks.toList, downgradeDagRequest)
        }
      }

      val schemaInferer: SchemaInfer = if (satisfyDowngradeThreshold) {
        // Should downgrade to full table scan for one region
        logger.info(
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

      val rowTransformer: RowTransformer = schemaInferer.getRowTransformer
      val outputTypes = output.map(_.dataType)
      val converters = outputTypes.map(CatalystTypeConverters.createToCatalystConverter)

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
          val sparkRow = TiConverter.toSparkRow(rowIterator.next(), rowTransformer)
          // Need to convert spark row to internal row for Catalyst
          project(rowToInternalRow(sparkRow, outputTypes, converters))
        }
      }
      resultIter
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val numHandles = longMetric("numHandles")
    val numIndexScanTasks = longMetric("numIndexScanTasks")
    val numDowngradedTasks = longMetric("numDowngradedTasks")
    val numRegions = longMetric("numRegions")
    val numIndexRangesScanned = longMetric("numIndexRangesScanned")
    val numDowngradeRangesScanned = longMetric("numDowngradeRangesScanned")

    val downgradeDagRequest = dagRequest.copy()
    // We need to clear index info in order to perform table scan
    downgradeDagRequest.clearIndexInfo()
    downgradeDagRequest.resetFilters(downgradeDagRequest.getDowngradeFilters)

    ReflectionMapPartitionWithIndexInternal(
      child.execute(),
      internalRowToUnsafeRowWithIndex(
        numOutputRows,
        numHandles,
        numIndexScanTasks,
        numDowngradedTasks,
        numRegions,
        numIndexRangesScanned,
        numDowngradeRangesScanned,
        downgradeDagRequest
      )
    ).invoke()
  }

  override def verboseString: String =
    s"TiSpark $nodeName{downgradeThreshold=$downgradeThreshold,downgradeFilter=${dagRequest.getFilters}"

  override def simpleString: String = verboseString
}
