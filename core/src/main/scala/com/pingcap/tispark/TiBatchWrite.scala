/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tispark

import com.pingcap.tikv.codec.{KeyUtils, TableCodec}
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{Key, RowKey}
import com.pingcap.tikv.meta.{TiDBInfo, TiTableInfo}
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer, KeyRangeUtils}
import com.pingcap.tikv.{TiBatchWriteUtils, _}
import com.pingcap.tispark.utils.TiUtil
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{Row, TiContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

/**
 * An ugly implementation of batch write framework, which will be
 * replaced by spark api.
 */
object TiBatchWrite {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType
  // TODO: port this val into conf
  // disabled because of this bug: https://internal.pingcap.net/jira/browse/TISPARK-127
  private val skipCommitSecondaryKey = false
  private val fraction = 0.01

  private def calcRDDSize(rdd: RDD[Row]): Long =
    // TODO: this is only approximate estimate
    // change to key value form for better approximation.
    rdd
      .map(_.mkString(",").getBytes("UTF-8").length.toLong)
      .reduce(_ + _) //add the sizes together

  private def estimateDataSize(sampledRDD: RDD[Row]) = {
    // get all data size
    val sampleRDDSize = calcRDDSize(sampledRDD)
    logger.info(s"sample data size is ${sampleRDDSize / (1024 * 1024)} MB")

    val sampleRowCount = sampledRDD.count()
    val sampleAvgRowSize = sampleRDDSize / sampleRowCount
    logger.info(s"sample avg row size is $sampleAvgRowSize")

    val estimatedTotalSize = (sampleRowCount * sampleAvgRowSize) / fraction
    val formatter = java.text.NumberFormat.getIntegerInstance

    val estimateInMB = estimatedTotalSize / (1024 * 1024)
    logger.info("estimatedTotalSize is %s MB".format(formatter.format(estimateInMB)))
    (estimateInMB.toLong, sampleRowCount)
  }

  def calculateSplitKeys(sampleRDD: RDD[Row],
                         estimatedTotalSize: Long,
                         sampleRDDCount: Long,
                         tblInfo: TiTableInfo,
                         isUpdate: Boolean,
                         regionSplitNumber: Option[Int]): List[SerializableKey] = {

    var regionNeed = (estimatedTotalSize / 96.0).toLong
    // update region split number if user want more region
    if (regionSplitNumber.isDefined) {
      if (regionSplitNumber.get > regionNeed) {
        regionNeed = regionSplitNumber.get
      }
    }
    // if region split is not needed, just return an empty list
    if (regionNeed == 0) return List.empty

    // TODO check this write is update or not
    val handleRdd: RDD[Long] = sampleRDD
      .map(sparkRow2TiKVRow)
      // TODO: fix allocator issue
      .map(row => extractHandleId(row, null, tblInfo, isUpdate = false))
      .sortBy(k => k)
    val step = sampleRDDCount / regionNeed
    val splitKeys = handleRdd
      .zipWithIndex()
      .filter {
        case (_, idx) => idx % step == 0
      }
      .map { _._1 }
      .map(h => new SerializableKey(RowKey.toRowKey(tblInfo.getId, h).getBytes))
      .collect()
      .toList

    logger.info("region need split %d times".format(splitKeys.length))
    splitKeys
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def writeToTiDB(rdd: RDD[SparkRow],
                  tableRef: TiTableReference,
                  tiContext: TiContext,
                  regionSplitNumber: Option[Int] = None,
                  enableRegionPreSplit: Boolean = false) {
    // initialize
    val tiConf = tiContext.tiConf
    val tiSession = tiContext.tiSession
    val kvClient = tiSession.createTxnClient()
    val (tiDBInfo, tiTableInfo, colDataTypes, colIds) = getDBAndTableInfo(tableRef, tiContext)
    val tiKVRowRDD = rdd.map(sparkRow2TiKVRow)
    // TODO: lock table
    // pending: https://internal.pingcap.net/jira/browse/TIDB-1628
    val step = tiKVRowRDD.count()
    val idAllocator =
      new IDAllocator(tiDBInfo.getId, tiSession.getCatalog, tiTableInfo.isAutoIncColUnsigned, step)

    if (enableRegionPreSplit) {
      logger.info("region pre split is enabled.")
      val sampleRDD = rdd.sample(withReplacement = false, fraction = fraction)
      val (dataSize, sampleRDDCount) = estimateDataSize(sampleRDD)

      tiContext.tiSession.splitRegionAndScatter(
        calculateSplitKeys(
          sampleRDD,
          dataSize,
          sampleRDDCount,
          tiTableInfo,
          isUpdate = false,
          regionSplitNumber
        ).map(k => k.bytes).asJava
      )
    }

    // shuffle data in same task which belong to same region
    val shuffledRDD = {
      shuffleKeyToSameRegion(tiKVRowRDD, idAllocator, tableRef, tiTableInfo, tiContext)
    }.cache()

    // take one row as primary key
    val (primaryKey: SerializableKey, primaryRow: TiRow) = {
      val takeOne = shuffledRDD.take(1)
      if (takeOne.length == 0) {
        logger.warn("there is no data in source rdd")
        return
      } else {
        takeOne(0)
      }
    }

    logger.info(s"primary key: $primaryKey primary row: $primaryRow")

    // filter primary key
    val finalWriteRDD = shuffledRDD.filter {
      case (key, _) => !key.equals(primaryKey)
    }

    // get timestamp as start_ts
    val startTs = kvClient.getTimestamp.getVersion
    logger.info(s"startTS: $startTs")

    // driver primary pre-write
    val ti2PCClient = new TwoPhaseCommitter(kvClient, startTs)
    val prewritePrimaryBackoff =
      ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)
    val encodedRow = encodeTiRow(primaryRow, colDataTypes, colIds)
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, encodedRow)

    // executors secondary pre-write
    finalWriteRDD.foreachPartition { iterator =>
      val tiSessionOnExecutor = new TiSession(tiConf)
      val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
      val ti2PCClientOnExecutor = new TwoPhaseCommitter(kvClientOnExecutor, startTs)
      val prewriteSecondaryBackoff =
        ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)

      val pairs = iterator.map {
        case (key, row) =>
          new TwoPhaseCommitter.BytePairWrapper(
            key.bytes,
            encodeTiRow(row, colDataTypes, colIds)
          )
      }.asJava

      ti2PCClientOnExecutor
        .prewriteSecondaryKeys(prewriteSecondaryBackoff, primaryKey.bytes, pairs)
    }

    // driver primary commit
    val commitTs = kvClient.getTimestamp.getVersion
    // check commitTS
    if (commitTs <= startTs) {
      throw new TiBatchWriteException(
        s"invalid transaction tso with startTs=$startTs, commitTs=$commitTs"
      )
    }
    val commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF)
    ti2PCClient.commitPrimaryKey(commitPrimaryBackoff, primaryKey.bytes, commitTs)

    // executors secondary commit
    if (!skipCommitSecondaryKey) {
      finalWriteRDD.foreachPartition { iterator =>
        val tiSessionOnExecutor = new TiSession(tiConf)
        val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
        val ti2PCClientOnExecutor = new TwoPhaseCommitter(kvClientOnExecutor, startTs)
        val commitSecondaryBackoff =
          ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF)

        val keys = iterator.map {
          case (key, _) => new TwoPhaseCommitter.ByteWrapper(key.bytes)
        }.asJava

        try {
          ti2PCClientOnExecutor.commitSecondaryKeys(commitSecondaryBackoff, keys, commitTs)
        } catch {
          case e: TiBatchWriteException =>
            // ignored
            logger.warn(s"commit secondary key error", e)
        }
      }
    } else {
      logger.info("skipping commit secondary key")
    }
  }

  @throws(classOf[NoSuchTableException])
  private def getDBAndTableInfo(
    tableRef: TiTableReference,
    tiContext: TiContext
  ): (TiDBInfo, TiTableInfo, Array[DataType], TLongArrayList) = {
    val tiDBInfo = tiContext.tiSession.getCatalog.getDatabase(tableRef.databaseName)
    val tiTableInfo =
      tiContext.tiSession.getCatalog.getTable(tableRef.databaseName, tableRef.tableName)
    if (tiTableInfo == null) {
      throw new NoSuchTableException(tableRef.databaseName, tableRef.tableName)
    }
    val tableColSize = tiTableInfo.getColumns.size()
    val dataTypes = new Array[DataType](tableColSize)
    val colIds = new TLongArrayList

    for (i <- 0 until tableColSize) {
      val tiColumnInfo = tiTableInfo.getColumn(i)
      colIds.add(tiColumnInfo.getId)
      dataTypes.update(i, tiColumnInfo.getType)
    }
    (tiDBInfo, tiTableInfo, dataTypes, colIds)
  }

  @throws(classOf[NoSuchTableException])
  private def shuffleKeyToSameRegion(rdd: RDD[TiRow],
                                     allocator: IDAllocator,
                                     tableRef: TiTableReference,
                                     tiTableInfo: TiTableInfo,
                                     tiContext: TiContext): RDD[(SerializableKey, TiRow)] = {
    val databaseName = tableRef.databaseName
    val tableName = tableRef.tableName
    val session = tiContext.tiSession
    val table = session.getCatalog.getTable(databaseName, tableName)
    if (table == null) {
      throw new NoSuchTableException(databaseName, tableName)
    }
    val tableId = table.getId

    val regions = getRegions(table, tiContext)
    val tiRegionPartitioner = new TiRegionPartitioner(regions)

    logger.info(
      s"find ${regions.size} regions in database: $databaseName table: $tableName tableId: $tableId"
    )

    rdd
      .map(row => (tiKVRow2Key(row, allocator, tableId, tiTableInfo, isUpdate = false), row))
      .groupByKey(tiRegionPartitioner)
      .map {
        case (key, iterable) =>
          // remove duplicate rows if key equals
          (key, iterable.head)
      }
  }

  private def getRegions(table: TiTableInfo, tiContext: TiContext): List[TiRegion] = {
    import scala.collection.JavaConversions._
    TiBatchWriteUtils.getRegionsByTable(tiContext.tiSession, table).toList
  }

  private def extractHandleId(row: TiRow,
                              allocator: IDAllocator,
                              tableInfo: TiTableInfo,
                              isUpdate: Boolean): Long = {
    // If handle ID is changed when update, update will remove the old record first,
    // and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    val handle = if (row.fieldCount > tableInfo.getColumns.size && !isUpdate) {
      row.getLong(row.fieldCount - 1)
    } else {
      val columnList = tableInfo.getColumns.asScala
      columnList.find(_.isPrimaryKey) match {
        case Some(primaryColumn) =>
          row.getLong(primaryColumn.getOffset)
        case None =>
          // pk is not handle case.
          allocator.alloc(tableInfo.getId)
      }
    }
    handle
  }

  private def tiKVRow2Key(row: TiRow,
                          allocator: IDAllocator,
                          tableId: Long,
                          tiTableInfo: TiTableInfo,
                          isUpdate: Boolean): SerializableKey =
    new SerializableKey(
      RowKey.toRowKey(tableId, extractHandleId(row, allocator, tiTableInfo, isUpdate)).getBytes
    )

  private def sparkRow2TiKVRow(sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    val tiRow = ObjectRowImpl.create(fieldCount)
    for (i <- 0 until fieldCount) {
      val data = sparkRow.get(i)
      val sparkDataType = sparkRow.schema(i).dataType
      val tiDataType = TiUtil.fromSparkType(sparkDataType)
      tiRow.set(i, tiDataType, data)
    }
    tiRow
  }

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow,
                          colDataTypes: Array[DataType],
                          colIDs: TLongArrayList): Array[Byte] = {
    val colSize = tiRow.fieldCount()
    val tableColSize = colDataTypes.length

    if (colSize != tableColSize) {
      throw new TiBatchWriteException(s"col size $colSize != table column size $tableColSize")
    }

    // TODO: ddl state change
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-82
    val values = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      values.update(i, tiRow.get(i, colDataTypes(i)))
    }

    TableCodec.encodeRow(colDataTypes, colIDs, values)
  }
}

class TiRegionPartitioner(regions: List[TiRegion]) extends Partitioner {
  override def numPartitions: Int = regions.length

  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    regions.indices.foreach { i =>
      val region = regions(i)
      val range = KeyRangeUtils.makeRange(region.getStartKey, region.getEndKey)
      if (range.contains(rawKey)) {
        return i
      }
    }
    0
  }
}

class SerializableKey(val bytes: Array[Byte]) extends Serializable {
  override def toString: String = KeyUtils.formatBytes(bytes)

  override def equals(that: Any): Boolean =
    that match {
      case that: SerializableKey => this.bytes.sameElements(that.bytes)
      case _                     => false
    }
}
