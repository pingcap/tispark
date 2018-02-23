package com.pingcap.tispark.statistics

import com.google.common.cache.{CacheBuilder, Weigher}
import com.pingcap.tikv.TiSession
import com.pingcap.tikv.expression.{ByItem, ColumnRef, ComparisonBinaryExpression, Constant}
import com.pingcap.tikv.key.{Key, RowKey, TypedKey}
import com.pingcap.tikv.kvproto.Coprocessor
import com.pingcap.tikv.meta.TiDAGRequest.PushDownType
import com.pingcap.tikv.meta.{TiColumnInfo, TiDAGRequest, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.row.Row
import com.pingcap.tikv.statistics._
import com.pingcap.tikv.types.{DataType, DataTypeFactory, MySQLType}
import com.pingcap.tikv.util.KeyRangeUtils
import com.pingcap.tispark.TiConfigConst
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.collection.mutable

private case class StatisticsDTO(colId: Long,
                                 isIndex: Int,
                                 distinct: Long,
                                 version: Long,
                                 nullCount: Long,
                                 dataType: DataType,
                                 rawCMSketch: Array[Byte],
                                 idxInfo: TiIndexInfo,
                                 colInfo: TiColumnInfo)

private case class StatisticsResult(histId: Long,
                                    histogram: Histogram,
                                    cMSketch: CMSketch,
                                    idxInfo: TiIndexInfo,
                                    colInfo: TiColumnInfo) {
  def hasIdxInfo: Boolean = idxInfo != null

  def hasColInfo: Boolean = colInfo != null
}

class StatisticsManager(tiSession: TiSession, maxBktPerTbl: Long = Long.MaxValue) {
  private lazy val snapshot = tiSession.createSnapshot()
  private lazy val catalog = tiSession.getCatalog
  private lazy val metaTable = catalog.getTable("mysql", "stats_meta")
  private lazy val histTable = catalog.getTable("mysql", "stats_histograms")
  private lazy val bucketTable = catalog.getTable("mysql", "stats_buckets")
  private final lazy val logger = LoggerFactory.getLogger(getClass.getName)
  private final val statisticsMap = CacheBuilder
    .newBuilder()
    .maximumWeight(maxBktPerTbl) // cache should not grow beyond a certain size
    .weigher(new Weigher[Long, TableStatistics] {
      override def weigh(key: Long, value: TableStatistics): Int = {
        // we calculate bucket number as weight. Weights are computed at entry creation time, and are static thereafter
        value.getColumnsHistMap.map(_._2.getHistogram.getBuckets.size).sum +
          value.getIndexHistMap.map(_._2.getHistogram.getBuckets.size).sum
      }
    })
    .build[Long, TableStatistics]

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

    val tblStatistic = if (statisticsMap.asMap().contains(tblId)) {
      statisticsMap.getIfPresent(tblId)
    } else {
      new TableStatistics(tblId)
    }

    val req = new TiDAGRequest(PushDownType.NORMAL)
    req.setTableInfo(histTable)
    val start = RowKey.createMin(histTable.getId)
    val end = RowKey.createBeyondMax(histTable.getId)
    val ranges = mutable.ArrayBuffer[Coprocessor.KeyRange]()
    ranges += KeyRangeUtils.makeCoprocRange(start.toByteString, end.toByteString)
    req.addFilter(
      ComparisonBinaryExpression
        .equal(ColumnRef.create("table_id"), Constant.create(tblId))
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
    if (!rows.hasNext) return

    val requests = rows.map((row: Row) => {
      val isIndex = if (row.getLong(1) > 0) true else false
      val histID = row.getLong(2)
      val distinct = row.getLong(3)
      val histVer = row.getLong(4)
      val nullCount = row.getLong(5)
      val cMSketch = row.getBytes(6)
      val indexInfos = table.getIndices.filter(_.getId == histID)
      val colInfos = table.getColumns.filter(_.getId == histID)

      // we should only query those columns that user specified before
      if (!loadAll && !neededColIds.contains(histID)) return

      var indexFlag = 1
      var dataType: DataType = DataTypeFactory.of(MySQLType.TypeBlob)
      // Columns info found
      if (!isIndex && colInfos.nonEmpty) {
        indexFlag = 0
        dataType = colInfos.head.getType
      } else if (colInfos.isEmpty || indexInfos.isEmpty) {
        logger.error(
          s"We cannot find histogram id $histID in table info ${table.getName} now. It may be deleted."
        )
        return
      }

      StatisticsDTO(
        histID,
        indexFlag,
        distinct,
        histVer,
        nullCount,
        dataType,
        cMSketch,
        indexInfos.head,
        colInfos.head
      )
    })
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

    statisticsMap.put(tblId, tblStatistic)
  }

  private def statisticsFromStorage(tableId: Long,
                                    requests: Seq[StatisticsDTO]): Seq[StatisticsResult] = {
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
    if (rows.isEmpty) return null
    // Group by hist_id(column_id)
    rows.toList
      .groupBy(_.getLong(7))
      .map((t: (Long, List[Row])) => {
        val histId = t._1
        val rows = t._2.iterator
        val matches = requests.filter(_.colId == histId)
        if (matches.nonEmpty) {
          val matched = matches.head
          var totalCount: Long = 0
          val buckets = mutable.ArrayBuffer[Bucket]()
          while (rows.hasNext) {
            val row = rows.next()
            val count = row.getLong(0)
            val repeats = row.getLong(1)
            var lowerBound: Key = null
            var upperBound: Key = null
            // all bounds are stored as blob in bucketTable currently, decode using blob type
            lowerBound =
              TypedKey.toTypedKey(row.getBytes(2), DataTypeFactory.of(MySQLType.TypeBlob))
            upperBound =
              TypedKey.toTypedKey(row.getBytes(3), DataTypeFactory.of(MySQLType.TypeBlob))
            totalCount += count
            buckets += new Bucket(totalCount, repeats, lowerBound, upperBound)
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
      })
      .filter(_ != null)
      .toSeq
  }

  def getTableStatistics(id: Long): TableStatistics = {
    statisticsMap.getIfPresent(id)
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
            session.conf.get(TiConfigConst.MAX_BUCKET_SIZE_PER_TABLE, "2000000").toLong
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
