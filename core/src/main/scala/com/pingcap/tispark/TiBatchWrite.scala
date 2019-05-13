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

import java.util

import com.pingcap.tikv.codec.{KeyUtils, TableCodec}
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{Key, RowKey}
import com.pingcap.tikv.meta.{TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.types.DataType
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer, KeyRangeUtils}
import com.pingcap.tikv.{TiBatchWriteUtils, _}
import com.pingcap.tispark.utils.TiUtil
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
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

  private def calcRDDSize(rdd: RDD[(SerializableKey, TiRow, Array[Byte])]): Long =
    rdd.map(_._3.length).reduce(_ + _)

  private def estimateDataSize(sampledRDD: RDD[(SerializableKey, TiRow, Array[Byte])],
                               options: TiDBOptions) = {
    // get all data size
    val sampleRDDSize = calcRDDSize(sampledRDD)
    logger.info(s"sample data size is ${sampleRDDSize / (1024 * 1024)} MB")

    val sampleRowCount = sampledRDD.count()
    val sampleAvgRowSize = sampleRDDSize / sampleRowCount
    logger.info(s"sample avg row size is $sampleAvgRowSize")

    val estimatedTotalSize = (sampleRowCount * sampleAvgRowSize) / options.sampleFraction
    val formatter = java.text.NumberFormat.getIntegerInstance

    val estimateInMB = estimatedTotalSize / (1024 * 1024)
    logger.info("estimatedTotalSize is %s MB".format(formatter.format(estimateInMB)))
    (estimateInMB.toLong, sampleRowCount)
  }

  private def calculateSplitKeys(sampleRDD: RDD[(SerializableKey, TiRow, Array[Byte])],
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
      .map(obj => extractHandleId(obj._2, tblInfo, isUpdate = false))
      .sortBy(k => k)
    val step = sampleRDDCount / regionNeed
    val splitKeys = handleRdd
      .zipWithIndex()
      .filter {
        case (_, idx) => idx % step == 0
      }
      .map {
        _._1
      }
      .map(h => new SerializableKey(RowKey.toRowKey(tblInfo.getId, h).getBytes))
      .collect()
      .toList

    logger.info("region need split %d times".format(splitKeys.length))
    splitKeys
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def writeToTiDB(rdd: RDD[SparkRow],
                  tiContext: TiContext,
                  options: TiDBOptions,
                  regionSplitNumber: Option[Int] = None,
                  enableRegionPreSplit: Boolean = false) {
    // check if write enable
    if (!tiContext.tiConf.isWriteEnable) {
      throw new TiBatchWriteException(
        "tispark batch write is disabled! set spark.tispark.write.enable to enable."
      )
    }

    // initialize
    val tiConf = tiContext.tiConf
    val tiSession = tiContext.tiSession
    val kvClient = tiSession.createTxnClient()
    val tableRef = TiTableReference(options.database, options.table)
    val (tiTableInfo, colDataTypes, colIds) = getTableInfo(tableRef, tiContext)

    // check unsupported
    unsupportCheck(tiTableInfo)

    // spark row to tikv row
    val tiKVRowRDD = rdd.map(sparkRow2TiKVRow)

    // deduplicate
    val deduplicateRDD = deduplicate(tiKVRowRDD, tableRef, tiTableInfo, tiContext, options)

    // encode tirow
    val encodedTiRowRDD = deduplicateRDD.map {
      case (key, tiRow) => (key, tiRow, encodeTiRow(tiRow, colDataTypes, colIds))
    }

    // region pre-split
    if (enableRegionPreSplit) {
      logger.info("region pre split is enabled.")
      val sampleRDD =
        encodedTiRowRDD.sample(withReplacement = false, fraction = options.sampleFraction)
      val (dataSize, sampleRDDCount) = estimateDataSize(sampleRDD, options)

      tiContext.tiSession.splitRegionAndScatter(
        calculateSplitKeys(
          encodedTiRowRDD,
          dataSize,
          sampleRDDCount,
          tiTableInfo,
          isUpdate = false,
          regionSplitNumber
        ).map(k => k.bytes).asJava
      )
    }

    // TODO: lock table
    // pending: https://internal.pingcap.net/jira/browse/TIDB-1628

    // shuffle data in same task which belong to same region
    val shuffledRDD = {
      shuffleKeyToSameRegion(encodedTiRowRDD, tableRef, tiTableInfo, tiContext)
    }.cache()

    // take one row as primary key
    val (primaryKey: SerializableKey, primaryRow: Array[Byte]) = {
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
    val ti2PCClient = new TiBatchWrite2PCClient(kvClient, startTs)
    val prewritePrimaryBackoff =
      ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, primaryRow)

    // executors secondary pre-write
    finalWriteRDD.foreachPartition { iterator =>
      val tiSessionOnExecutor = new TiSession(tiConf)
      val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
      val ti2PCClientOnExecutor = new TiBatchWrite2PCClient(kvClientOnExecutor, startTs)
      val prewriteSecondaryBackoff =
        ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)

      val pairs = iterator.map {
        case (key, row) =>
          new TiBatchWrite2PCClient.BytePairWrapper(key.bytes, row)
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
    if (!options.skipCommitSecondaryKey) {
      finalWriteRDD.foreachPartition { iterator =>
        val tiSessionOnExecutor = new TiSession(tiConf)
        val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
        val ti2PCClientOnExecutor = new TiBatchWrite2PCClient(kvClientOnExecutor, startTs)
        val commitSecondaryBackoff =
          ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF)

        val keys = iterator.map {
          case (key, _) => new TiBatchWrite2PCClient.ByteWrapper(key.bytes)
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

  @throws(classOf[TiBatchWriteException])
  private def unsupportCheck(tiTableInfo: TiTableInfo): Unit = {
    // write to table with secondary index (KEY & UNIQUE KEY)
    for (i <- 0 until tiTableInfo.getIndices.size()) {
      val tiIndexInfo = tiTableInfo.getIndices.get(i)
      if (!tiIndexInfo.isPrimary) {
        throw new TiBatchWriteException(
          "tispark currently does not support write data table with secondary index(KEY & UNIQUE KEY)!"
        )
      }
    }

    // write to partition table
    if (tiTableInfo.isPartitionEnabled) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to partition table!"
      )
    }

    // table with primary key & primary key is not handle (TINYINT、SMALLINT、MEDIUMINT、INTEGER)
    if (tiTableInfo.hasPrimaryKey && !tiTableInfo.isPkHandle) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to table with primary key, but type is not TINYINT、SMALLINT、MEDIUMINT、INTEGER!"
      )
    }
  }

  @throws(classOf[NoSuchTableException])
  private def getTableInfo(tableRef: TiTableReference,
                           tiContext: TiContext): (TiTableInfo, Array[DataType], TLongArrayList) = {
    val tiTableInfo =
      tiContext.tiSession.getCatalog.getTable(tableRef.databaseName, tableRef.tableName)
    if (tiTableInfo == null) {
      throw new NoSuchTableException(tableRef.databaseName, tableRef.tableName)
    }
    val tableColSize = tiTableInfo.getColumns.size()
    val dataTypes = new Array[DataType](tableColSize)
    val ids = new TLongArrayList

    for (i <- 0 until tableColSize) {
      val tiColumnInfo = tiTableInfo.getColumn(i)
      ids.add(tiColumnInfo.getId)
      dataTypes.update(i, tiColumnInfo.getType)
    }
    (tiTableInfo, dataTypes, ids)
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  private def deduplicate(rdd: RDD[TiRow],
                          tableRef: TiTableReference,
                          tiTableInfo: TiTableInfo,
                          tiContext: TiContext,
                          options: TiDBOptions): RDD[(SerializableKey, TiRow)] = {
    val databaseName = tableRef.databaseName
    val tableName = tableRef.tableName
    val session = tiContext.tiSession
    val table = session.getCatalog.getTable(databaseName, tableName)
    if (table == null) {
      throw new NoSuchTableException(databaseName, tableName)
    }
    val tableId = table.getId

    val shuffledRDD = rdd
      .map(row => (tiKVRow2Key(row, tableId, tiTableInfo, isUpdate = false), row))
      .groupByKey()

    if (!options.deduplicate) {
      val duplicateCountRDD = shuffledRDD.flatMap {
        case (_, iterable) =>
          if (iterable.size > 1) {
            Some(iterable.size)
          } else {
            None
          }
      }

      if (!duplicateCountRDD.isEmpty()) {
        throw new TiBatchWriteException("data conflicts! set the parameter deduplicate.")
      }
    }

    shuffledRDD.map {
      case (key, iterable) =>
        // remove duplicate rows if key equals
        (key, iterable.head)
    }
  }

  @throws(classOf[NoSuchTableException])
  private def shuffleKeyToSameRegion(rdd: RDD[(SerializableKey, TiRow, Array[Byte])],
                                     tableRef: TiTableReference,
                                     tiTableInfo: TiTableInfo,
                                     tiContext: TiContext): RDD[(SerializableKey, Array[Byte])] = {
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
      .map(obj => (obj._1, obj._3))
      .groupByKey(tiRegionPartitioner)
      .map {
        case (key, iterable) =>
          // remove duplicate rows if key equals (should not happen, cause already deduplicated)
          (key, iterable.head)
      }
  }

  private def getRegions(table: TiTableInfo, tiContext: TiContext): List[TiRegion] = {
    import scala.collection.JavaConversions._
    TiBatchWriteUtils.getRegionsByTable(tiContext.tiSession, table).toList
  }

  private def extractHandleId(row: TiRow, tiTableInfo: TiTableInfo, isUpdate: Boolean): Long = {
    // If handle ID is changed when update, update will remove the old record first,
    // and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    val tblInfo = tiTableInfo.copyTableWithOutRowId()
    val handle = if (row.fieldCount > tblInfo.getColumns.size && !isUpdate) {
      row.getLong(row.fieldCount - 1)
    } else {
      val columnList = tiTableInfo.getColumns.asScala
      columnList.find(_.isPrimaryKey) match {
        case Some(primaryColumn) =>
          row.getLong(primaryColumn.getOffset)

        case None =>
          // TODO: auto generate a primary key if does not exists
          // pending: https://internal.pingcap.net/jira/browse/TISPARK-70
          try {
            row.getLong(0)
          } catch {
            case _: Throwable => row.getInteger(0)
          }
      }
    }
    handle
  }

  private def tiKVRow2Key(row: TiRow,
                          tableId: Long,
                          tiTableInfo: TiTableInfo,
                          isUpdate: Boolean): SerializableKey =
    new SerializableKey(
      RowKey.toRowKey(tableId, extractHandleId(row, tiTableInfo, isUpdate)).getBytes
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

  override def hashCode(): Int =
    util.Arrays.hashCode(bytes)
}
