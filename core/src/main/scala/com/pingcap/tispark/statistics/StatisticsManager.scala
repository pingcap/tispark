/*
 *
 * Copyright 2018 PingCAP, Inc.
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
 *
 */

package com.pingcap.tispark.statistics

import com.google.common.cache.CacheBuilder
import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiColumnInfo, TiDAGRequest, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.row.Row
import com.pingcap.tikv.statistics._
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.{Snapshot, TiSession}
import com.pingcap.tispark.statistics.StatisticsHelper.shouldUpdateHistogram
import com.pingcap.tispark.statistics.estimate.{DefaultTableSizeEstimator, TableSizeEstimator}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

private[statistics] case class StatisticsDTO(
    colId: Long,
    isIndex: Int,
    distinct: Long,
    version: Long,
    nullCount: Long,
    dataType: DataType,
    rawCMSketch: Array[Byte],
    idxInfo: TiIndexInfo,
    colInfo: TiColumnInfo)

private[statistics] case class StatisticsResult(
    histId: Long,
    histogram: Histogram,
    cMSketch: CMSketch,
    idxInfo: TiIndexInfo,
    colInfo: TiColumnInfo) {
  def hasIdxInfo: Boolean = idxInfo != null

  def hasColInfo: Boolean = colInfo != null
}

/**
 * Manager class for maintaining table statistics information cache.
 *
 * Statistics information is useful for index selection and broadcast join support in TiSpark currently,
 * and these are arranged follows:
 *
 * `statisticsMap` contains `tableId`->TableStatistics data, each table(id) will have a TableStatistics
 * if you have loaded statistics information successfully.
 */
object StatisticsManager {

  private final lazy val logger = LoggerFactory.getLogger(getClass.getName)
  private final val statisticsMap = CacheBuilder
    .newBuilder()
    .build[java.lang.Long, TableStatistics]
  protected var initialized: Boolean = false
  private var session: TiSession = _
  private var snapshot: Snapshot = _
  private var catalog: Catalog = _
  private var dbPrefix: String = _
  // An estimator used to calculate table size.
  private var tableSizeEstimator: TableSizeEstimator = _
  // Statistics information table columns explanation:
  // stats_meta:
  //       Version       | A time stamp assigned by pd, updates along with DDL updates.
  //       Count         | Number of rows in the table, if equals to -1, that means this table may had been removed.
  //       Modify_count  | Indicates the count lose during update procedure, which shows the `healthiness` of the table.表示Table在更新过程中损失的Count，表示表的“健康度”
  // stats_histograms:
  //       Version       | Indicate version of this column's histogram.
  //       IsIndex       | Indicate whether this column is index.
  //       HistID        | Index id or column id, related to `IsIndex` above.
  //       Null Count    | The number of `NULL`.
  //       Distinct Count| Distinct value count.
  //       Modify Count  | Modification count, not used currently.
  // stats_buckets:
  //       TableID IsIndex HistID BucketID | Intuitive columns.
  //       Count         | The number of all the values that falls on the bucket and the previous buckets.
  //       Lower_Bound   | Minimal value of this bucket.
  //       Upper_Bound   | Maximal value of this bucket.
  //       Repeats       | The repeat count of maximal value.
  //
  // More explanation could be found here
  // https://github.com/pingcap/docs/blob/master/sql/statistics.md
  private[statistics] var metaTable: TiTableInfo = _
  private[statistics] var histTable: TiTableInfo = _
  private[statistics] var bucketTable: TiTableInfo = _

  /**
   * Load statistics information maintained by TiDB to TiSpark.
   *
   * @param table   The table whose statistics info is needed.
   * @param columns Concerning columns for `table`, only these columns' statistics information
   *                will be loaded, if empty, all columns' statistics info will be loaded
   */
  def loadStatisticsInfo(table: TiTableInfo, columns: String*): Unit =
    synchronized {
      require(table != null, "TableInfo should not be null")
      if (!StatisticsHelper.isManagerReady) {
        logger.warn("Some of the statistics information table are not loaded properly, " +
          "make sure you have executed analyze table command before these information could be used by TiSpark.")
        return
      }

      // TODO load statistics by pid
      val tblId = table.getId
      val tblCols = table.getColumns
      val loadAll = columns == null || columns.isEmpty
      var neededColIds = mutable.ArrayBuffer[Long]()
      if (!loadAll) {
        // check whether input column could be found in the table
        columns.distinct.foreach((col: String) => {
          val isColValid = tblCols.exists(_.matchName(col))
          if (!isColValid) {
            throw new RuntimeException(s"Column $col cannot be found in table ${table.getName}")
          } else {
            neededColIds += tblCols.find(_.matchName(col)).get.getId
          }
        })
      }

      // use cached one for incremental update
      val tblStatistic = if (statisticsMap.asMap.containsKey(tblId)) {
        statisticsMap.getIfPresent(tblId)
      } else {
        new TableStatistics(tblId)
      }

      try {
        loadStatsFromStorage(tblId, tblStatistic, table, loadAll, neededColIds)
      } catch {
        case _: Throwable => // ignored
      }
    }

  private def loadStatsFromStorage(
      tblId: Long,
      tblStatistic: TableStatistics,
      table: TiTableInfo,
      loadAll: Boolean,
      neededColIds: mutable.ArrayBuffer[Long]): Unit = {
    // load count, modify_count, version info
    loadMetaToTblStats(tblId, tblStatistic)
    val req = StatisticsHelper
      .buildHistogramsRequest(histTable, tblId, session.getTimestamp)

    val rows = readDAGRequest(req, histTable.getId)
    if (rows.isEmpty) return

    val requests = rows
      .map { StatisticsHelper.extractStatisticsDTO(_, table, loadAll, neededColIds, histTable) }
      .filter { _ != null }
    val results = statisticsResultFromStorage(tblId, requests.toSeq)

    // Update cache
    results.foreach { putOrUpdateTblStats(tblStatistic, _) }

    statisticsMap.put(tblId, tblStatistic)
  }

  private def putOrUpdateTblStats(tblStatistic: TableStatistics, result: StatisticsResult): Unit =
    if (result.hasIdxInfo) {
      val oldIdxSts = tblStatistic.getIndexHistMap.putIfAbsent(
        result.histId,
        new IndexStatistics(result.histogram, result.cMSketch, result.idxInfo))
      if (shouldUpdateHistogram(oldIdxSts, result)) {
        oldIdxSts.setHistogram { result.histogram }
        oldIdxSts.setCmSketch { result.cMSketch }
        oldIdxSts.setIndexInfo { result.idxInfo }
      }
    } else if (result.hasColInfo) {
      val oldColSts = tblStatistic.getColumnsHistMap
        .putIfAbsent(
          result.histId,
          new ColumnStatistics(
            result.histogram,
            result.cMSketch,
            result.histogram.totalRowCount.toLong,
            result.colInfo))
      if (shouldUpdateHistogram(oldColSts, result)) {
        oldColSts.setHistogram { result.histogram }
        oldColSts.setCmSketch { result.cMSketch }
        oldColSts.setColumnInfo { result.colInfo }
      }
    }

  private def loadMetaToTblStats(tableId: Long, tableStatistics: TableStatistics): Unit = {
    val req =
      StatisticsHelper.buildMetaRequest(metaTable, tableId, session.getTimestamp)

    val rows = readDAGRequest(req, metaTable.getId)
    if (rows.isEmpty) return

    val row = rows.next()
    tableStatistics.setVersion { row.getUnsignedLong(0) }
    tableStatistics.setModifyCount { row.getLong(2) }
    tableStatistics.setCount { row.getUnsignedLong(3) }
  }

  private[statistics] def readDAGRequest(req: TiDAGRequest, physicalId: Long): Iterator[Row] =
    snapshot.tableReadRow(req, physicalId)

  private def statisticsResultFromStorage(
      tableId: Long,
      requests: Seq[StatisticsDTO]): Seq[StatisticsResult] = {
    val req =
      StatisticsHelper.buildBucketRequest(bucketTable, tableId, session.getTimestamp)

    val rows = readDAGRequest(req, bucketTable.getId)
    if (rows.isEmpty) return Nil
    // Group by hist_id(column_id)
    rows.toList
      .groupBy { _.getLong(2) }
      .flatMap { t: (Long, List[Row]) =>
        val histId = t._1
        val rowsById = t._2
        // split bucket rows into index rows / non-index rows
        val (idxRows, colRows) = rowsById.partition { _.getLong(1) > 0 }
        val (idxReq, colReq) = requests.partition { _.isIndex > 0 }
        Array(
          StatisticsHelper.extractStatisticResult(histId, idxRows.iterator, idxReq),
          StatisticsHelper.extractStatisticResult(histId, colRows.iterator, colReq))
      }
      .filter { _ != null }
      .toSeq
  }

  def getTableStatistics(id: Long): TableStatistics =
    statisticsMap.getIfPresent(id)

  /**
   * Estimated row count of one table
   * @param table table to evaluate
   * @return estimated number of rows in this table
   */
  def estimatedRowCount(table: TiTableInfo): Long = tableSizeEstimator.estimatedCount(table)

  /**
   * Estimated table size in bytes using statistic info.
   *
   * @param table table to estimate
   * @return estimated table size in bytes
   */
  def estimateTableSize(table: TiTableInfo): Long = tableSizeEstimator.estimatedTableSize(table)

  def setEstimator(estimator: TableSizeEstimator): Unit = tableSizeEstimator = estimator

  def initStatisticsManager(tiSession: TiSession): Unit =
    if (!initialized) {
      synchronized {
        if (!initialized) {
          initialize(tiSession)
          initialized = true
        }
      }
    }

  protected def initialize(tiSession: TiSession): Unit = {
    session = tiSession
    snapshot = tiSession.createSnapshot()
    catalog = tiSession.getCatalog
    dbPrefix = tiSession.getConf.getDBPrefix
    // An estimator used to calculate table size.
    tableSizeEstimator = DefaultTableSizeEstimator
    metaTable = catalog.getTable(s"${dbPrefix}mysql", "stats_meta")
    histTable = catalog.getTable(s"${dbPrefix}mysql", "stats_histograms")
    bucketTable = catalog.getTable(s"${dbPrefix}mysql", "stats_buckets")
    statisticsMap.invalidateAll()
  }

  def reset(): Unit = initialized = false
}
