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

import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.codec.{KeyUtils, TableCodec}
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{Key, RowKey}
import com.pingcap.tikv.meta.{TiColumnInfo, TiDBInfo, TiTableInfo}
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.txn.TxnKVClient
import com.pingcap.tikv.types.{DataType, IntegerType}
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer, KeyRangeUtils}
import com.pingcap.tikv.{TiBatchWriteUtils, _}
import com.pingcap.tispark.utils.TiUtil
import gnu.trove.list.array.TLongArrayList
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, TiContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object TiBatchWrite {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def writeToTiDB(df: DataFrame,
                  tiContext: TiContext,
                  options: TiDBOptions,
                  regionSplitNumber: Option[Int] = None,
                  enableRegionPreSplit: Boolean = false): Unit =
    new TiBatchWrite(df, tiContext, options, regionSplitNumber, enableRegionPreSplit).write()
}

class TiBatchWrite(@transient val df: DataFrame,
                   @transient val tiContext: TiContext,
                   options: TiDBOptions,
                   regionSplitNumber: Option[Int],
                   enableRegionPreSplit: Boolean)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import TiBatchWrite._

  private var tiConf: TiConfiguration = _
  @transient private var tiSession: TiSession = _
  @transient private var kvClient: TxnKVClient = _

  private var tiTableRef: TiTableReference = _
  private var tiDBInfo: TiDBInfo = _
  private var tiTableInfo: TiTableInfo = _

  private var dfColSize: Int = _
  private var tableColSize: Int = _
  private var dfTiColumnInfo: Array[TiColumnInfo] = _

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  private def write(): Unit = {
    // check if write enable
    if (!tiContext.tiConf.isWriteEnable) {
      throw new TiBatchWriteException(
        "tispark batch write is disabled! set spark.tispark.write.enable to enable."
      )
    }

    // check empty
    if (df.count() == 0) {
      logger.warn("data is empty!")
      return
    }

    // initialize
    tiConf = tiContext.tiConf
    tiSession = tiContext.tiSession
    kvClient = tiSession.createTxnClient()
    tiTableRef = options.tiTableRef
    tiDBInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
    tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)
    if (tiTableInfo == null) {
      throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
    }
    dfColSize = df.columns.length
    tableColSize = tiTableInfo.getColumns.size()

    // check check check
    checkUnsupported()
    checkColumnNumbers()

    // initialize dfTiColumnInfo
    dfTiColumnInfo = new Array[TiColumnInfo](dfColSize)
    for (i <- 0 until dfColSize) {
      if (dfColSize == tableColSize - 1) {
        // auto increment case, use auto generated id
        val offset = tiTableInfo.getAutoIncrementColInfo.getOffset
        if (i < offset) {
          dfTiColumnInfo(i) = tiTableInfo.getColumn(i)
        } else {
          dfTiColumnInfo(i) = tiTableInfo.getColumn(i + 1)
        }
      } else {
        dfTiColumnInfo(i) = tiTableInfo.getColumn(i)
      }
    }

    // continue check check check
    checkValueNotNull()

    // TODO: lock table
    // pending: https://internal.pingcap.net/jira/browse/TIDB-1628

    // TODO: if this write is update, TiDB reuses row_id. We need adopt this behavior.
    // TODO: 1. fill column with its default value if it is null
    //       2. refactor the writing process

    // when primary is handle, it does not require allocate ids for each row.
    val offset = if (!tiTableInfo.isPkHandle) {
      val rowIDAllocator = RowIDAllocator.create(
        tiDBInfo.getId,
        tiTableInfo.getId,
        tiSession.getCatalog,
        tiTableInfo.isAutoIncColUnsigned,
        df.count
      )
      rowIDAllocator.getStart
    } else {
      0
    }

    // i + start will be handle id if primary key is not handle
    val tiKVRowRDD = df.rdd.zipWithIndex.map {
      case (row, i) =>
        sparkRow2TiKVRow(i + offset, row)
    }

    // deduplicate
    val deduplicateRDD =
      deduplicate(tiKVRowRDD)

    // encode TiROW
    val encodedTiRowRDD = deduplicateRDD.map {
      case (key, tiRow) => (key, tiRow, encodeTiRow(tiRow))
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

    // shuffle data in same task which belong to same region
    val shuffledRDD = shuffleKeyToSameRegion(encodedTiRowRDD).cache()

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
    val ti2PCClient = new TwoPhaseCommitter(kvClient, startTs)
    val prewritePrimaryBackoff =
      ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, primaryRow)

    // executors secondary pre-write
    finalWriteRDD.foreachPartition { iterator =>
      val tiSessionOnExecutor = new TiSession(tiConf)
      val kvClientOnExecutor = tiSessionOnExecutor.createTxnClient()
      val ti2PCClientOnExecutor = new TwoPhaseCommitter(kvClientOnExecutor, startTs)
      val prewriteSecondaryBackoff =
        ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)

      val pairs = iterator.map {
        case (key, row) =>
          new TwoPhaseCommitter.BytePairWrapper(key.bytes, row)
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

  @throws(classOf[TiBatchWriteException])
  private def checkUnsupported(): Unit = {
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

  private def checkColumnNumbers(): Unit = {
    if (!tiTableInfo.hasAutoIncrementColumn && dfColSize != tableColSize) {
      throw new TiBatchWriteException(
        s"table without auto increment column, but data col size $dfColSize != table column size $tableColSize"
      )
    }

    if (tiTableInfo.hasAutoIncrementColumn && dfColSize != tableColSize && dfColSize != tableColSize - 1) {
      throw new TiBatchWriteException(
        s"table with auto increment column, but data col size $dfColSize != table column size $tableColSize and table column size - 1 ${tableColSize - 1} "
      )
    }
  }

  private def checkValueNotNull(): Unit = {
    var notNullColumnIndex: List[Int] = Nil

    for (i <- 0 until dfColSize) {
      val tiColumnInfo = dfTiColumnInfo(i)
      if (tiColumnInfo.getType.isNotNull) {
        notNullColumnIndex = i :: notNullColumnIndex
      }
    }

    if (notNullColumnIndex.nonEmpty) {
      val encoder = RowEncoder(df.schema)
      val nullRowCount = df
        .flatMap { row =>
          var result: Option[SparkRow] = None
          notNullColumnIndex.foreach { col =>
            if (row.isNullAt(col)) {
              result = Some(row)
            }
          }
          result
        }(encoder)
        .count()
      if (nullRowCount > 0) {
        throw new TiBatchWriteException(
          s"Insert null value to not null column! $nullRowCount rows contain illegal null values!"
        )
      }
    }
  }

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
      .map(obj => extractHandleId(obj._2, isUpdate = false))
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

  @throws(classOf[TiBatchWriteException])
  private def deduplicate(rdd: RDD[TiRow]): RDD[(SerializableKey, TiRow)] = {
    val tableId = tiTableInfo.getId

    val shuffledRDD: RDD[(SerializableKey, Iterable[TiRow])] = rdd
      .map(row => (tiKVRow2Key(row, isUpdate = false), row))
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
  private def shuffleKeyToSameRegion(
    rdd: RDD[(SerializableKey, TiRow, Array[Byte])]
  ): RDD[(SerializableKey, Array[Byte])] = {
    val tableId = tiTableInfo.getId

    val regions = getRegions
    val tiRegionPartitioner = new TiRegionPartitioner(regions)

    logger.info(
      s"find ${regions.size} regions in $tiTableRef tableId: $tableId"
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

  private def getRegions: List[TiRegion] = {
    import scala.collection.JavaConversions._
    TiBatchWriteUtils.getRegionsByTable(tiSession, tiTableInfo).toList
  }

  private def extractHandleId(row: TiRow, isUpdate: Boolean): Long = {
    // If handle ID is changed when update, update will remove the old record first,
    // and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    val handle = if (row.fieldCount > tableColSize && !isUpdate) {
      row.getLong(row.fieldCount - 1)
    } else {
      val primaryKeyColumn = tiTableInfo.getPrimaryKeyColumn
      if (primaryKeyColumn == null) {
        throw new TiBatchWriteException("should never happen")
      } else {
        // it is a workaround. pk is handle must be a number
        row
          .get(primaryKeyColumn.getOffset, primaryKeyColumn.getType)
          .asInstanceOf[Number]
          .longValue()
      }
    }
    handle
  }

  private def tiKVRow2Key(row: TiRow, isUpdate: Boolean): SerializableKey =
    new SerializableKey(
      RowKey.toRowKey(tiTableInfo.getId, extractHandleId(row, isUpdate)).getBytes
    )

  private def sparkRow2TiKVRow(handleId: Long, sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    var tiRow: TiRow = null
    if (tiTableInfo.getPrimaryKeyColumn != null) {
      if (tiTableInfo.isPkHandle) {
        tiRow = ObjectRowImpl.create(fieldCount)
        for (i <- 0 until fieldCount) {
          val data = sparkRow.get(i)
          val sparkDataType = sparkRow.schema(i).dataType
          val tiDataType = TiUtil.fromSparkType(sparkDataType)
          tiRow.set(i, tiDataType, data)
        }
      } else if (tiTableInfo.getAutoIncrementColInfo != null) {
        // when column is auto_increment, it must has a primary key
        // its value will be filled at the beginning according tidb's logic.
        val autoincrementCol = tiTableInfo.getAutoIncrementColInfo
        tiRow = ObjectRowImpl.create(fieldCount)
        for (i <- 0 until fieldCount) {
          var data = sparkRow.get(i)
          val sparkDataType = sparkRow.schema(i).dataType
          val colName = sparkRow.schema(i).name
          // check do we need fill auto increment column
          if (colName.equals(autoincrementCol.getName)) {
            // only update auto increment column when the value is null
            if (data == null) {
              data = handleId
            }
          }
          val tiDataType = TiUtil.fromSparkType(sparkDataType)
          tiRow.set(i, tiDataType, data)
        }
      } else {
        throw new TiBatchWriteException(
          "cannot insert on a table with primary key but it's not a handle" +
            "or auto increment"
        )
      }
    } else {
      // table does not have primary key, we can just insert and do not need consider
      // the constraint of primary key.
      tiRow = ObjectRowImpl.create(fieldCount + 1)
      for (i <- 0 until fieldCount) {
        val data = sparkRow.get(i)
        val sparkDataType = sparkRow.schema(i).dataType
        val tiDataType = TiUtil.fromSparkType(sparkDataType)
        tiRow.set(i, tiDataType, data)
      }
      // append _tidb_rowid at the end
      tiRow.set(fieldCount, IntegerType.BIGINT, handleId)
    }
    tiRow
  }

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    var colSize = tiRow.fieldCount()
    val columnInfos = tiTableInfo.getColumns

    val colDataTypes = new Array[DataType](columnInfos.size)
    val colIds = new TLongArrayList

    for (i <- 0 until columnInfos.size) {
      val tiColumnInfo = tiTableInfo.getColumn(i)
      colIds.add(tiColumnInfo.getId)
      colDataTypes.update(i, tiColumnInfo.getType)
    }

    val tableColSize = colDataTypes.length

    // an hidden row _tidb_rowid may exist
    if (colSize > (tableColSize + 1)) {
      throw new TiBatchWriteException(
        s"data col size $colSize > table column size $tableColSize + 1"
      )
    }
    val hasHiddenRow = colSize == tableColSize + 1

    // when we have an hidden row, we do not need
    // write such column into TiKV
    if (hasHiddenRow) {
      colSize = colSize - 1
    }
    // TODO: ddl state change
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-82
    val values = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      // pk is handle can be skipped
      values.update(i, tiRow.get(i, colDataTypes(i)))
    }

    TableCodec.encodeRow(columnInfos, colIds, values, tiTableInfo.isPkHandle)
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
