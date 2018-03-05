/*
 *
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
 *
 */

package com.pingcap.tispark.statistics

import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, Weigher}
import com.google.common.util.concurrent.ListenableFuture
import com.pingcap.tikv.TiSession
import com.pingcap.tikv.meta.{TiColumnInfo, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.row.Row
import com.pingcap.tikv.statistics._
import com.pingcap.tikv.types.DataType
import com.pingcap.tispark.TiConfigConst
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

private[statistics] case class StatisticsDTO(colId: Long,
                                             isIndex: Int,
                                             distinct: Long,
                                             version: Long,
                                             nullCount: Long,
                                             dataType: DataType,
                                             rawCMSketch: Array[Byte],
                                             idxInfo: TiIndexInfo,
                                             colInfo: TiColumnInfo)

private[statistics] case class StatisticsResult(histId: Long,
                                                histogram: Histogram,
                                                cMSketch: CMSketch,
                                                idxInfo: TiIndexInfo,
                                                colInfo: TiColumnInfo) {
  def hasIdxInfo: Boolean = idxInfo != null

  def hasColInfo: Boolean = colInfo != null
}

class StatisticsManager(tiSession: TiSession,
                        maxBktPerTbl: Long = Long.MaxValue,
                        refreshAfterWrite: Long = Long.MaxValue) {
  private lazy val snapshot = tiSession.createSnapshot()
  private lazy val catalog = tiSession.getCatalog
  private lazy val metaTable = catalog.getTable("mysql", "stats_meta")
  private lazy val histTable = catalog.getTable("mysql", "stats_histograms")
  private lazy val bucketTable = catalog.getTable("mysql", "stats_buckets")
  private final lazy val logger = LoggerFactory.getLogger(getClass.getName)
  private final val cacheLoader = new CacheLoader[Object, Object] {
    override def load(tblIdObj: Object): Object = {
      val tblId = tblIdObj.asInstanceOf[Long]
      val tblStatistic = new TableStatistics(tblId)
      null
    }

    override def reload(tableId: Object, tableStatistics: Object): ListenableFuture[Object] = {
      null
    }
  }

  private final val statisticsMap = CacheBuilder
    .newBuilder()
    .refreshAfterWrite(refreshAfterWrite, TimeUnit.MINUTES)
    .build(cacheLoader)

  def tableStatsFromStorage(table: TiTableInfo, columns: String*): Unit = synchronized {
    require(table != null, "TableInfo should not be null")

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

    val tblStatistic = if (statisticsMap.asMap.containsKey(tblId)) {
      statisticsMap.getIfPresent(tblId).asInstanceOf[TableStatistics]
    } else {
      new TableStatistics(tblId)
    }

    // load count, modify_count, version info
    metaFromStorage(tblId, tblStatistic)
    val req =
      StatisticsHelper.buildHistogramsRequest(histTable, tblId, snapshot.getTimestamp.getVersion)

    val rows = snapshot.tableRead(req)
    if (!rows.hasNext) return

    val requests = rows
      .map(StatisticsHelper.extractStatisticsDTO(_, table, loadAll, neededColIds))
      .filter(_ != null)
    val results = statisticsFromStorage(tblId, requests.toSeq)

    results.foreach((result: StatisticsResult) => {
      if (result.hasIdxInfo)
        tblStatistic.getIndexHistMap
          .put(
            result.histId,
            new IndexStatistics(result.histogram, result.cMSketch, result.idxInfo)
          )
      else if (result.hasColInfo)
        tblStatistic.getColumnsHistMap
          .put(
            result.histId,
            new ColumnStatistics(
              result.histogram,
              result.cMSketch,
              result.histogram.totalRowCount.toLong,
              result.colInfo
            )
          )
    })

    statisticsMap.put(tblId.asInstanceOf[Object], tblStatistic.asInstanceOf[Object])
  }

  private def metaFromStorage(tableId: Long, tableStatistics: TableStatistics): Unit = {
    val req =
      StatisticsHelper.buildMetaRequest(metaTable, tableId, snapshot.getTimestamp.getVersion)

    val rows = snapshot.tableRead(req)
    if (rows.isEmpty) return

    val row = rows.next()
    tableStatistics.setCount(row.getLong(1))
    tableStatistics.setModifyCount(row.getLong(2))
    tableStatistics.setVersion(row.getLong(3))
  }

  private def statisticsFromStorage(tableId: Long,
                                    requests: Seq[StatisticsDTO]): Seq[StatisticsResult] = {
    val req =
      StatisticsHelper.buildBucketRequest(bucketTable, tableId, snapshot.getTimestamp.getVersion)

    val rows = snapshot.tableRead(req)
    if (rows.isEmpty) return Nil
    // Group by hist_id(column_id)
    rows.toList
      .groupBy(_.getLong(7))
      .map((t: (Long, List[Row])) => {
        val histId = t._1
        val rows = t._2.iterator
        StatisticsHelper.extractStatisticResult(histId, rows, requests)
      })
      .filter(_ != null)
      .toSeq
  }

  def getTableStatistics(id: Long): TableStatistics = {
    statisticsMap.getIfPresent(id).asInstanceOf[TableStatistics]
  }

  def getTableCount(id: Long): Long = {
    val tbStst = getTableStatistics(id)
    if (tbStst != null) {
      tbStst.getCount
    } else {
      Long.MaxValue
    }
  }

  def invalidateAll(): Unit = {
    statisticsMap.invalidateAll()
  }

  def invalidate(table: TiTableInfo): Unit = {
    statisticsMap.invalidate(table.getId)
  }
}

object StatisticsManager {
  private var manager: StatisticsManager = _

  def initStatisticsManager(tiSession: TiSession, session: SparkSession): Unit = {
    if (manager == null) {
      synchronized {
        if (manager == null) {
          manager = new StatisticsManager(
            tiSession,
            session.conf.get(TiConfigConst.MAX_BUCKET_SIZE_PER_TABLE, "2000000000").toLong,
            session.conf.get(TiConfigConst.CACHE_EXPIRE_AFTER_ACCESS, "43200").toLong
          )
        }
      }
    }
  }

  def getInstance(): StatisticsManager = {
    if (manager == null) {
      throw new RuntimeException("StatisticsManager has not been initialized properly.")
    }
    manager
  }
}
