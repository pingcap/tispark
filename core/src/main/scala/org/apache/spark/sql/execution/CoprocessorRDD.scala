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

import com.pingcap.tikv.columnar.{TiChunk, TiColumnarBatchHelper}
import com.pingcap.tikv.meta.{TiDAGRequest, TiTimestamp}
import com.pingcap.tikv.operation.iterator.CoprocessorIterator
import com.pingcap.tikv.util.RangeSplitter.RegionTask
import com.pingcap.tikv.util.{KeyRangeUtils, RangeSplitter}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.listener.CacheInvalidateListener
import com.pingcap.tispark.utils.ReflectionUtil.ReflectionMapPartitionWithIndexInternal
import com.pingcap.tispark.utils.TiUtil
import gnu.trove.list.array
import gnu.trove.list.array.TLongArrayList
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.tispark.TiRDD
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.tikv.kvproto.Coprocessor.KeyRange

import scala.collection.JavaConversions._
import scala.collection.mutable

trait LeafColumnarExecRDD extends LeafExecNode {
  override val outputPartitioning: Partitioning = UnknownPartitioning(0)
  private[execution] val tiRDDs: List[TiRDD]

  def dagRequest: TiDAGRequest = tiRDDs.head.dagRequest

  override def verboseString: String =
    if (tiRDDs.size > 1) {
      val b = new mutable.StringBuilder()
      b.append(s"TiSpark $nodeName on partition table:\n")
      tiRDDs.zipWithIndex.map {
        case (_, i) => b.append(s"partition p$i")
      }
      b.append(s"with dag request: $dagRequest")
      b.toString()
    } else {
      val engine = if (dagRequest.getUseTiFlash) {
        "TiFlash"
      } else {
        "TiKV"
      }
      s"$engine $nodeName{$dagRequest}" +
        s"${TiUtil.getReqEstCountStr(dagRequest)}"
    }

  override def simpleString: String = verboseString
}

case class ColumnarCoprocessorRDD(output: Seq[Attribute], tiRDDs: List[TiRDD], fetchHandle: Boolean)
    extends LeafColumnarExecRDD
    with ColumnarBatchScan {
  override val outputPartitioning: Partitioning = UnknownPartitioning(0)

  private[execution] val internalRDDs: List[RDD[InternalRow]] = tiRDDs

  override def dagRequest: TiDAGRequest = tiRDDs.head.dagRequest

  override val nodeName: String = if (fetchHandle) {
    "FetchHandleRDD"
  } else {
    "CoprocessorRDD"
  }

  override protected def doExecute(): RDD[InternalRow] = {
    if (!fetchHandle) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      sparkContext.union(internalRDDs)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(sparkContext.union(internalRDDs))
}

/**
 * RegionTaskExec is used for issuing requests which are generated based
 * on handles retrieved from [[ColumnarCoprocessorRDD]].
 *
 * RegionTaskExec will downgrade a index scan plan to table scan plan if handles retrieved from one
 * region exceed spark.tispark.plan.downgrade.index_threshold in your spark config.
 *
 * Refer to code in [[com.pingcap.tispark.TiDBRelation]] and [[ColumnarCoprocessorRDD]] for further details.
 *
 */
case class ColumnarRegionTaskExec(child: SparkPlan,
                                  output: Seq[Attribute],
                                  dagRequest: TiDAGRequest,
                                  tiConf: TiConfiguration,
                                  ts: TiTimestamp,
                                  @transient private val session: TiSession,
                                  @transient private val sparkSession: SparkSession)
    extends UnaryExecNode
    with ColumnarBatchScan {

  override lazy val metrics: Map[String, SQLMetric] = Map(
    "scanTime" -> SQLMetrics.createTimingMetric(sparkContext, "scan time"),
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

  override val nodeName: String = "RegionTaskExec"
  // cache invalidation call back function
  // used for driver to update PD cache
  private val callBackFunc: CacheInvalidateListener = CacheInvalidateListener.getInstance()

  override protected def doExecute(): RDD[InternalRow] = {
    WholeStageCodegenExec(this)(codegenStageId = 0).execute()
  }

  def fetchTableResultsFromHandles(
    numOutputRows: SQLMetric,
    numHandles: SQLMetric,
    numIndexScanTasks: SQLMetric,
    numDowngradedTasks: SQLMetric,
    numRegions: SQLMetric,
    numIndexRangesScanned: SQLMetric,
    numDowngradeRangesScanned: SQLMetric,
    downgradeDagRequest: TiDAGRequest
  ): (Int, Iterator[InternalRow]) => Iterator[InternalRow] = { (_, iter) =>
    // For each partition, we do some initialization work
    val logger = Logger.getLogger(getClass.getName)
    val session = TiSession.getInstance(tiConf)
    session.injectCallBackFunc(callBackFunc)
    val batchSize = tiConf.getIndexScanBatchSize
    val downgradeThreshold = tiConf.getDowngradeThreshold

    iter.flatMap { row =>
      val handles = row.getArray(1).toLongArray()
      val handleIterator: util.Iterator[Long] = handles.iterator
      var taskCount = 0
      numRegions += 1

      val completionService =
        new ExecutorCompletionService[util.Iterator[TiChunk]](
          session.getThreadPoolForIndexScan
        )
      var rowIterator: util.Iterator[TiChunk] = null

      // After `splitAndSortHandlesByRegion`, ranges in the task are arranged in order
      def generateIndexTasks(handles: TLongArrayList): util.List[RegionTask] = {
        val indexTasks: util.List[RegionTask] = new util.ArrayList[RegionTask]()
        indexTasks.addAll(
          RangeSplitter
            .newSplitter(session.getRegionManager)
            .splitAndSortHandlesByRegion(dagRequest.getIds, handles)
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

      def isTaskRangeSizeInvalid(task: RegionTask): Boolean =
        task == null ||
          task.getRanges.size() > tiConf.getMaxRequestKeyRangeSize

      def submitTasks(tasks: List[RegionTask], dagRequest: TiDAGRequest): Unit = {
        taskCount += 1
        val task = new Callable[util.Iterator[TiChunk]] {
          override def call(): util.Iterator[TiChunk] = {
            CoprocessorIterator.getTiChunkIterator(dagRequest, tasks, session)
          }

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
            numIndexRangesScanned += task.getRanges.size
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

      if (satisfyDowngradeThreshold) {
        // Should downgrade to full table scan for one region
        logger.info(
          s"Index scan task range size = ${indexTaskRanges.size}, " +
            s"exceeding downgrade threshold = $downgradeThreshold, " +
            s"index scan handle size = ${handles.length}, will try to merge."
        )
        doDowngradeScan(indexTaskRanges.toList)
      } else {
        // Request doesn't need to be downgraded
        doIndexScan()
      }

      // The result iterator serves as an wrapper to the final result we fetched from region tasks
      new Iterator[ColumnarBatch] {
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

        override def next(): ColumnarBatch = {
          TiColumnarBatchHelper.createColumnarBatch(rowIterator.next())
        }
      }.asInstanceOf[Iterator[InternalRow]]
    }
  }

  private def inputRDD(): RDD[InternalRow] = {
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
      fetchTableResultsFromHandles(
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

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD())
}
