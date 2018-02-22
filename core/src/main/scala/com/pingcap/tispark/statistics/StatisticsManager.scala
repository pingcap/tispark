package com.pingcap.tispark.statistics

import com.google.common.cache.CacheBuilder
import com.pingcap.tikv.TiSession
import com.pingcap.tikv.expression.{ByItem, ColumnRef, ComparisonBinaryExpression, Constant}
import com.pingcap.tikv.key.{Key, RowKey, TypedKey}
import com.pingcap.tikv.kvproto.Coprocessor
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo}
import com.pingcap.tikv.statistics._
import com.pingcap.tikv.types.{DataType, DataTypeFactory, MySQLType}
import com.pingcap.tikv.util.KeyRangeUtils
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

class StatisticsManager(tiSession: TiSession, maxBktPerTbl: Long = Long.MaxValue) {
  private lazy val snapshot = tiSession.createSnapshot()
  private lazy val catalog = tiSession.getCatalog
  private lazy val conf = tiSession.getConf
  private lazy val metaTable = catalog.getTable("mysql", "stats_meta")
  private lazy val histTable = catalog.getTable("mysql", "stats_histograms")
  private lazy val bucketTable = catalog.getTable("mysql", "stats_buckets")
  private final lazy val logger = LoggerFactory.getLogger(getClass.getName)
  private final val statisticsMap = CacheBuilder.newBuilder()
    .maximumWeight(maxBktPerTbl) // cache should not grow beyond a certain size
    .weigher((_: Long, value: TableStatistics) => { // we calculate bucket number as weight
      value.getColumnsHistMap.map(_._2.getHistogram.getBuckets.size).sum +
        value.getIndexHistMap.map(_._2.getHistogram.getBuckets.size).sum
    })
    .build[Long, TableStatistics]

  def tableStatsFromStorage(table: TiTableInfo): Unit = {
    require(table != null, "TableInfo should not be null")
    statisticsMap.put(table.getId, new TableStatistics(table.getId))
    val req = new TiDAGRequest(PushDownType.NORMAL)
    req.setTableInfo(histTable)
    val start = RowKey.createMin(histTable.getId)
    val end = RowKey.createBeyondMax(histTable.getId)
    val ranges = mutable.ArrayBuffer[Coprocessor.KeyRange]()
    ranges += KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString)
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("table_id"), Constant.create(table.getId))
    )
    req.addRequiredColumn(ColumnRef.create("table_id"))
    req.addRequiredColumn(ColumnRef.create("is_index"))
    req.addRequiredColumn(ColumnRef.create("hist_id"))
    req.addRequiredColumn(ColumnRef.create("distinct_count"))
    req.addRequiredColumn(ColumnRef.create("version"))
    req.addRequiredColumn(ColumnRef.create("null_count"))
    req.addRequiredColumn(ColumnRef.create("cm_sketch"))
    req.addRanges(ranges)
    req.setStartTs(snapshot.getTimestamp.getVersion)
    req.resolve()

    val rows = snapshot.tableRead(req)
    if (!rows.hasNext) {
      return
    }

    while (rows.hasNext) {
      val row = rows.next()
      val isIndex = if (row.getLong(1) > 0) true else false
      val histID = row.getLong(2)
      val distinct = row.getLong(3)
      val histVer = row.getLong(4)
      val nullCount = row.getLong(5)
      val indexInfos = table.getIndices.filter(_.getId == histID)
      val colInfos = table.getColumns.filter(_.getId == histID)

      var indexFlag = 1
      var dataType: DataType = DataTypeFactory.of(MySQLType.TypeBlob)
      // Columns info found
      if (!isIndex && colInfos.nonEmpty) {
        indexFlag = 0
        dataType = colInfos.head.getType
      } else if (!isIndex || indexInfos.isEmpty) {
        logger.warn(
          s"We cannot find histogram id $histID in table info ${table.getName} now. It may be deleted."
        )
        return
      }
      val histogram = histogramFromStorage(
        table.getId,
        histID,
        indexFlag,
        distinct,
        histVer,
        nullCount,
        dataType
      )
      val cms = cmSketchFromStorage(table.getId, indexFlag, histID)
      if (isIndex && histogram != null) {
        statisticsMap.getIfPresent(table.getId).getIndexHistMap
          .put(histID, new IndexStatistics(histogram, cms, indexInfos.head))
      } else if (histogram != null) {
        statisticsMap.getIfPresent(table.getId).getColumnsHistMap
          .put(
            histID,
            new ColumnStatistics(histogram, cms, histogram.totalRowCount.toLong, colInfos.head)
          )
      }
    }
  }

  // TODO: Version number may overflow since it's uint64
  private def histogramFromStorage(tableId: Long,
                                   colId: Long,
                                   isIndex: Int,
                                   distinct: Long,
                                   version: Long,
                                   nullCount: Long,
                                   dataType: DataType): Histogram = {
    val req = new TiDAGRequest(PushDownType.NORMAL)
    req.setTableInfo(bucketTable)
    val start = RowKey.createMin(bucketTable.getId)
    val end = RowKey.createBeyondMax(bucketTable.getId)
    val ranges = mutable.ArrayBuffer[Coprocessor.KeyRange]()
    ranges += KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString)
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("table_id"), Constant.create(tableId))
    )
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("is_index"), Constant.create(isIndex))
    )
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("hist_id"), Constant.create(colId))
    )
    req.addOrderByItem(ByItem.create(ColumnRef.create("bucket_id"), false))
    req.setLimit(Int.MaxValue)
    req.addRequiredColumn(ColumnRef.create("count"))
    req.addRequiredColumn(ColumnRef.create("repeats"))
    req.addRequiredColumn(ColumnRef.create("lower_bound"))
    req.addRequiredColumn(ColumnRef.create("upper_bound"))
    req.addRequiredColumn(ColumnRef.create("bucket_id"))
    req.addRequiredColumn(ColumnRef.create("table_id"))
    req.addRequiredColumn(ColumnRef.create("is_index"))
    req.addRequiredColumn(ColumnRef.create("hist_id"))
    req.addRanges(ranges)
    req.setStartTs(snapshot.getTimestamp.getVersion)
    req.resolve()
    val rows = snapshot.tableRead(req)
    if (rows.isEmpty) {
      return null
    }
    var totalCount: Long = 0
    val buckets = mutable.ArrayBuffer[Bucket]()
    while (rows.hasNext) {
      val row = rows.next()
      val count = row.getLong(0)
      val repeats = row.getLong(1)
      var lowerBound: Key = null
      var upperBound: Key = null
      lowerBound = TypedKey.toTypedKey(row.getBytes(2), DataTypeFactory.of(MySQLType.TypeBlob))
      upperBound = TypedKey.toTypedKey(row.getBytes(3), DataTypeFactory.of(MySQLType.TypeBlob))
      totalCount += count
      buckets += new Bucket(totalCount, repeats, lowerBound, upperBound)
    }
    Histogram
      .newBuilder()
      .setId(colId)
      .setNDV(distinct)
      .setNullCount(nullCount)
      .setLastUpdateVersion(version)
      .setBuckets(buckets)
      .build()
  }

  private def cmSketchFromStorage(tableId: Long, isIndex: Int, histId: Long): CMSketch = {
    val req = new TiDAGRequest(PushDownType.NORMAL)
    req.setTableInfo(histTable)
    val start = RowKey.createMin(histTable.getId)
    val end = RowKey.createBeyondMax(histTable.getId)
    val ranges = mutable.ArrayBuffer[Coprocessor.KeyRange]()
    ranges += KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString)
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("table_id"), Constant.create(tableId))
    )
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("is_index"), Constant.create(isIndex))
    )
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("hist_id"), Constant.create(histId))
    )
    req.addRequiredColumn(ColumnRef.create("cm_sketch"))
    req.addRequiredColumn(ColumnRef.create("table_id"))
    req.addRequiredColumn(ColumnRef.create("is_index"))
    req.addRequiredColumn(ColumnRef.create("hist_id"))
    req.addRanges(ranges)
    req.setStartTs(snapshot.getTimestamp.getVersion)
    req.resolve()
    val rows = snapshot.tableRead(req)
    if (!rows.hasNext) {
      return null
    }
    val rawData = rows.next().getBytes(0)
    if (rawData == null || rawData.length <= 0) {
      return null
    }
    val sketch = com.pingcap.tidb.tipb.CMSketch.parseFrom(rawData)
    val result = CMSketch.newCMSketch(sketch.getRowsCount, sketch.getRows(0).getCountersCount)
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

  def getTableStatistics(id: Long): TableStatistics = {
    statisticsMap.getIfPresent(id)
  }
}

object StatisticsManager {
  private var manager: StatisticsManager = _

  def initStatisticsManager(tiSession: TiSession): Unit = {
    if (manager == null) {
      synchronized {
        if (manager == null) {
          manager = new StatisticsManager(tiSession)
        }
      }
    }
  }

  def getInstance(): StatisticsManager = synchronized {
    if (manager == null) {
      throw new RuntimeException("StatisticsManager has not been initialized completely.")
    }
    manager
  }
}