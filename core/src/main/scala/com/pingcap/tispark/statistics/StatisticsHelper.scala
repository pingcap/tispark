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

import com.google.common.primitives.UnsignedLong
import com.pingcap.tikv.expression.{ByItem, ColumnRef, ComparisonBinaryExpression, Constant}
import com.pingcap.tikv.key.{Key, TypedKey}
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo}
import com.pingcap.tikv.row.Row
import com.pingcap.tikv.statistics._
import com.pingcap.tikv.types.{DataType, DataTypeFactory, MySQLType}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

object StatisticsHelper {
  private final lazy val logger = LoggerFactory.getLogger(getClass.getName)
  private val metaRequiredCols = Seq(
    "table_id",
    "count",
    "modify_count",
    "version"
  )
  private val histRequiredCols = Seq(
    "table_id",
    "is_index",
    "hist_id",
    "distinct_count",
    "version",
    "null_count",
    "cm_sketch"
  )
  private val bucketRequiredCols = Seq(
    "count",
    "repeats",
    "lower_bound",
    "upper_bound",
    "bucket_id",
    "table_id",
    "is_index",
    "hist_id"
  )

  private[statistics] def isManagerReady(manager: StatisticsManager): Boolean =
    manager.metaTable != null &&
      manager.bucketTable != null &&
      manager.histTable != null

  private[statistics] def extractStatisticsDTO(row: Row,
                                               table: TiTableInfo,
                                               loadAll: Boolean,
                                               neededColIds: mutable.ArrayBuffer[Long],
                                               histTable: TiTableInfo): StatisticsDTO = {
    if (row.fieldCount() < 6) return null
    val isIndex = row.getLong(1) > 0
    val histID = row.getLong(2)
    val distinct = row.getLong(3)
    val histVer = row.getUnsignedLong(4)
    val nullCount = row.getLong(5)
    val cMSketch = if (checkColExists(histTable, "cm_sketch")) row.getBytes(6) else null
    // get index/col info for StatisticsDTO
    val indexInfos = table.getIndices
      .filter { _.getId == histID }

    val colInfos = table.getColumns
      .filter { _.getId == histID }

    var needed = true

    // we should only query those columns that user specified before
    if (!loadAll && !neededColIds.contains(histID)) needed = false

    var indexFlag = 1
    var dataType: DataType = DataTypeFactory.of(MySQLType.TypeBlob)
    // Columns info found
    if (!isIndex && colInfos.nonEmpty) {
      indexFlag = 0
      dataType = colInfos.head.getType
    } else if (!isIndex || indexInfos.isEmpty) {
      logger.warn(
        s"Cannot find histogram id $histID in table info ${table.getName} now. It may be deleted."
      )
      needed = false
    }

    if (needed) {
      StatisticsDTO(
        histID,
        indexFlag,
        distinct,
        histVer,
        nullCount,
        dataType,
        cMSketch,
        if (indexInfos.nonEmpty) indexInfos.head else null,
        if (colInfos.nonEmpty) colInfos.head else null
      )
    } else {
      null
    }
  }

  private[statistics] def shouldUpdateHistogram(statistics: ColumnStatistics,
                                                result: StatisticsResult): Boolean = {
    if (statistics == null || result == null) return false
    shouldUpdateHistogram(statistics.getHistogram, result.histogram)
  }

  private[statistics] def shouldUpdateHistogram(statistics: IndexStatistics,
                                                result: StatisticsResult): Boolean = {
    if (statistics == null || result == null) return false
    shouldUpdateHistogram(statistics.getHistogram, result.histogram)
  }

  /**
   * Check whether histogram should be updated according to version
   */
  private[statistics] def shouldUpdateHistogram(oldHis: Histogram, newHis: Histogram): Boolean = {
    if (oldHis == null || newHis == null) return false
    val oldVersion = UnsignedLong.fromLongBits(oldHis.getLastUpdateVersion)
    val newVersion = UnsignedLong.fromLongBits(newHis.getLastUpdateVersion)
    oldVersion.compareTo(newVersion) < 0
  }

  private[statistics] def extractStatisticResult(histId: Long,
                                                 rows: Iterator[Row],
                                                 requests: Seq[StatisticsDTO]): StatisticsResult = {
    val matches = requests.filter(_.colId == histId)
    if (matches.nonEmpty) {
      val matched = matches.head
      var totalCount: Long = 0
      val buckets = mutable.ArrayBuffer[Bucket]()
      while (rows.hasNext) {
        val row = rows.next()
        val isRowIndex = if (row.getLong(6) > 0) true else false
        val isRequestIndex = matched.isIndex > 0
        // if required DTO type(index/non index) is the same with the row
        if (isRequestIndex == isRowIndex) {
          val count = row.getLong(0)
          val repeats = row.getLong(1)
          var lowerBound: Key = null
          var upperBound: Key = null
          // all bounds are stored as blob in bucketTable currently, decode using blob type
          lowerBound = TypedKey.toTypedKey(row.getBytes(2), DataTypeFactory.of(MySQLType.TypeBlob))
          upperBound = TypedKey.toTypedKey(row.getBytes(3), DataTypeFactory.of(MySQLType.TypeBlob))
          totalCount += count
          buckets += new Bucket(totalCount, repeats, lowerBound, upperBound)
        }
      }
      // create histogram for column `colId`
      val histogram = Histogram
        .newBuilder()
        .setId(matched.colId)
        .setNDV(matched.distinct)
        .setNullCount(matched.nullCount)
        .setLastUpdateVersion(matched.version)
        .setBuckets(buckets)
        .build()
      // parse CMSketch
      val rawData = matched.rawCMSketch
      val cMSketch = if (rawData == null || rawData.length <= 0) {
        null
      } else {
        val sketch = com.pingcap.tidb.tipb.CMSketch.parseFrom(rawData)
        val result =
          CMSketch.newCMSketch(sketch.getRowsCount, sketch.getRows(0).getCountersCount)
        for (i <- 0 until sketch.getRowsCount) {
          val row = sketch.getRows(i)
          result.setCount(0)
          for (j <- 0 until row.getCountersCount) {
            val counter = row.getCounters(j)
            result.getTable()(i)(j) = counter
            result.setCount(result.getCount + counter)
          }
        }
        result
      }
      StatisticsResult(histId, histogram, cMSketch, matched.idxInfo, matched.colInfo)
    } else {
      null
    }
  }

  private[statistics] def buildHistogramsRequest(histTable: TiTableInfo,
                                                 targetTblId: Long,
                                                 startTs: Long): TiDAGRequest = {
    TiDAGRequest.Builder
      .newBuilder()
      .setFullTableScan(histTable)
      .addFilter(
        ComparisonBinaryExpression
          .equal(ColumnRef.create("table_id"), Constant.create(targetTblId))
      )
      .addRequiredCols(
        histRequiredCols.filter(checkColExists(histTable, _))
      )
      .setStartTs(startTs)
      .build(PushDownType.NORMAL)
  }

  private def checkColExists(table: TiTableInfo, column: String): Boolean =
    table.getColumns.exists { _.matchName(column) }

  private[statistics] def buildMetaRequest(metaTable: TiTableInfo,
                                           targetTblId: Long,
                                           startTs: Long): TiDAGRequest = {
    TiDAGRequest.Builder
      .newBuilder()
      .setFullTableScan(metaTable)
      .addFilter(
        ComparisonBinaryExpression
          .equal(ColumnRef.create("table_id"), Constant.create(targetTblId))
      )
      .addRequiredCols(metaRequiredCols.filter(checkColExists(metaTable, _)))
      .setStartTs(startTs)
      .build(PushDownType.NORMAL)
  }

  private[statistics] def buildBucketRequest(bucketTable: TiTableInfo,
                                             targetTblId: Long,
                                             startTs: Long): TiDAGRequest = {
    TiDAGRequest.Builder
      .newBuilder()
      .setFullTableScan(bucketTable)
      .addFilter(
        ComparisonBinaryExpression
          .equal(ColumnRef.create("table_id"), Constant.create(targetTblId))
      )
      .setLimit(Int.MaxValue)
      .addOrderBy(ByItem.create(ColumnRef.create("bucket_id"), false))
      .addRequiredCols(
        bucketRequiredCols.filter(checkColExists(bucketTable, _))
      )
      .setStartTs(startTs)
      .build(PushDownType.NORMAL)
  }
}
