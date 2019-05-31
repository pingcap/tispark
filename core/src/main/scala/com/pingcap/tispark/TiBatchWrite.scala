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
import java.util.Objects

import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.codec.{CodecDataOutput, KeyUtils, TableCodec}
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{IndexKey, Key, RowKey}
import com.pingcap.tikv.meta.{TiColumnInfo, TiDBInfo, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.txn.TxnKVClient
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer, KeyRangeUtils}
import com.pingcap.tikv.{TiBatchWriteUtils, _}
import com.pingcap.tispark.TiBatchWrite.TiRow
import com.pingcap.tispark.utils.TiConverter
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row, TiContext}
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

  private var uniqueIndices: List[TiIndexInfo] = _
  private var handleCol: TiColumnInfo = _

  private def calculateSplitKeys(sampleRDD: RDD[Row],
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
      .map(obj => extractHandleId(obj))
      .sortBy(k => k)
    val step = sampleRDDCount / regionNeed
    val splitKeys = handleRdd.zipWithIndex
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

  private def calcRDDSize(rdd: RDD[Row]): Long =
    rdd
      .map(_.mkString(",").getBytes("UTF-8").length.toLong)
      .reduce(_ + _) //add the sizes together

  private def estimateDataSize(sampledRDD: RDD[Row], options: TiDBOptions) = {
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
    uniqueIndices = tiTableInfo.getIndices.asScala.filter(index => index.isUnique).toList
    handleCol = tiTableInfo.getPrimaryKeyColumn

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

    // region pre-split
    if (enableRegionPreSplit && handleCol != null) {
      logger.info("region pre split is enabled.")
      val sampleRDD =
        df.rdd.sample(withReplacement = false, fraction = options.sampleFraction)
      val (dataSize, sampleRDDCount) = estimateDataSize(sampleRDD, options)

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

    val tiRowRdd = df.rdd.map(row => sparkRow2TiKVRow(row))

    val deduplicatedTiRowRdd = deduplicateIfNecessary(tiRowRdd, tiTableInfo.isPkHandle)

    // TODO support insert ignore
    val toBeCheckedRdd = getKeysNeedCheck(deduplicatedTiRowRdd)
    val checkedConflictRdd = checkConflictWithInsertKeys(toBeCheckedRdd)
    val encodedTiRowRDD = generateRDDToBeInserted(checkedConflictRdd)

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
  private def checkUnsupported(): Unit =
    // write to partition table
    if (tiTableInfo.isPartitionEnabled) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to partition table!"
      )
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

  // currently deduplicate can only perform on pk is handle table.
  @throws(classOf[TiBatchWriteException])
  private def deduplicateIfNecessary(rdd: RDD[TiRow], necessary: Boolean): RDD[TiRow] =
    if (necessary) {
      val shuffledRDD = rdd.map(row => (tiKVRow2Key(row), row))
      val rddGroupByKey = shuffledRDD.groupByKey
      if (!options.deduplicate) {
        val duplicateCountRDD = rddGroupByKey.flatMap {
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

      rddGroupByKey.map {
        case (_, iterable) =>
          // remove duplicate rows if key equals
          iterable.head
      }
    } else {
      rdd
    }

  @throws(classOf[NoSuchTableException])
  private def shuffleKeyToSameRegion(
    rdd: RDD[(SerializableKey, Array[Byte])]
  ): RDD[(SerializableKey, Array[Byte])] = {
    val tableId = tiTableInfo.getId

    val regions = getRegions
    val tiRegionPartitioner = new TiRegionPartitioner(regions)

    logger.info(
      s"find ${regions.size} regions in $tiTableRef tableId: $tableId"
    )

    rdd
      .map(obj => (obj._1, obj._2))
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

  private def extractHandleId(row: Row): Long =
    // If handle ID is changed when update, update will remove the old record first,
    // and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    if (tiTableInfo.isPkHandle) {
      // it is a workaround. pk is handle must be a number
      row
        .get(handleCol.getOffset)
        .asInstanceOf[Number]
        .longValue()
    } else {
      throw new TiBatchWriteException("cannot extract handle non pk is handle table")
    }

  private def extractHandleId(row: TiRow): Long =
    // If handle ID is changed when update, update will remove the old record first,
    // and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    if (tiTableInfo.isPkHandle) {
      // it is a workaround. pk is handle must be a number
      row
        .get(handleCol.getOffset, handleCol.getType)
        .asInstanceOf[Number]
        .longValue()
    } else {
      throw new TiBatchWriteException("cannot extract handle non pk is handle table")
    }

  private def tiKVRow2Key(row: TiRow): SerializableKey =
    new SerializableKey(
      RowKey.toRowKey(tiTableInfo.getId, extractHandleId(row)).getBytes
    )

  // convert spark's row to tikv row. We do not allocate handle for no pk case.
  // allocating handle id will be finished after we check conflict.
  private def sparkRow2TiKVRow(sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    val tiRow = ObjectRowImpl.create(fieldCount)
    for (i <- 0 until fieldCount) {
      val data = sparkRow.get(i)
      val sparkDataType = sparkRow.schema(i).dataType
      val tiDataType = TiConverter.fromSparkType(sparkDataType)
      tiRow.set(i, tiDataType, data)
    }
    tiRow
  }

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    var colSize = tiRow.fieldCount()

    // an hidden row _tidb_rowid may exist
    if (colSize > (tableColSize + 1)) {
      throw new TiBatchWriteException(
        s"data col size $colSize > table column size $tableColSize + 1"
      )
    }
    // TODO: remove hashHiddenRow later, it is not used any more.
    val hasHiddenRow = colSize == tableColSize + 1

    // when we have an hidden row, we do not need
    // write such column into TiKV
    if (hasHiddenRow) {
      colSize = colSize - 1
    }
    // TODO: ddl state change
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-82
    val convertedValues = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      // pk is handle can be skipped
      val columnInfo = tiTableInfo.getColumn(i)
      val value = tiRow.get(i, columnInfo.getType)
      val convertedValue = TiConverter.convertToTiDBType(columnInfo, value)
      convertedValues.update(i, convertedValue)
    }

    TableCodec.encodeRow(tiTableInfo.getColumns, convertedValues, tiTableInfo.isPkHandle)
  }

  private def generateRDDToBeInserted(
    rdd: RDD[ToBeCheckedRow]
  ) = {
    var allocatedRdd: RDD[(ToBeCheckedRow, Long)] = null
    if (!tiTableInfo.isPkHandle) {
      val step = rdd.count
      val catalog = TiSessionCache.getSession(tiConf).getCatalog
      val start = RowIDAllocator
        .create(tiDBInfo.getId, tiTableInfo.getId, catalog, tiTableInfo.isAutoIncColUnsigned, step)
        .getStart
      allocatedRdd = rdd.zipWithIndex.map {
        case (data, i) =>
          val handle = i + start
          (data, handle)
      }
    } else {
      allocatedRdd = rdd.map { data =>
        val handle =
          data.row.get(handleCol.getOffset, handleCol.getType).asInstanceOf[Number].longValue()
        (data, handle)
      }
    }

    val rowKeyAllocatedRdd = allocatedRdd.map {
      case (data, handle) =>
        (
          new SerializableKey(RowKey.toRowKey(tiTableInfo.getId, handle).getBytes),
          encodeTiRow(data.row)
        )
    }

    val indexKeyAllocatedRdd = allocatedRdd
      .map {
        case (data, handle) =>
          // extract index key from keyWithDupInfo
          val indexKeys = data.indexKeys.map { key =>
            key.key
          }
          indexKeys.map { key =>
            val cdo = new CodecDataOutput()
            cdo.writeLong(handle)
            (key, cdo.toBytes)
          }
      }
      .flatMap(identity)

    rowKeyAllocatedRdd.union(indexKeyAllocatedRdd)
  }

  private def checkConflictWithInsertKeys(
    rdd: RDD[ToBeCheckedRow]
  ) = {
    if (handleCol != null) {
      // data is conflict with TiKV
      val handleConflict = !rdd
        .filter {
          // check handle column and unique indices
          data =>
            data.handleKey.oldRow != null
        }
        .isEmpty()

      if (handleConflict) {
        throw new TiBatchWriteException("data to be inserted is conflict on primary key")
      }

      val handleDuplicated = rdd
        .map { row =>
          (row.handleKey, 0)
        }
        .groupByKey()
        .flatMap {
          case (_, iterable) =>
            if (iterable.size > 1) {
              Some(iterable.size)
            } else {
              None
            }
        }

      if (!handleDuplicated.isEmpty) {
        throw new TiBatchWriteException("data to be inserted is conflict on primary key")
      }
    }

    if (uniqueIndices.nonEmpty) {
      val handleConflict = !rdd
        .filter {
          // check handle column and unique indices
          data =>
            data.indexKeys.exists(p => p.oldHandle != -1)
        }
        .isEmpty()

      if (handleConflict) {
        throw new TiBatchWriteException("data to be inserted is conflict on index key")
      }
      val indexConflict = rdd
        .flatMap { row =>
          row.indexKeys.map { key =>
            (key, row)
          }
        }
        .groupByKey()
        .flatMap {
          case (_, iterable) =>
            if (iterable.size > 1) {
              Some(iterable.size)
            } else {
              None
            }
        }

      if (!indexConflict.isEmpty) {
        throw new TiBatchWriteException("data to be inserted is conflict on unique index")
      }
    }

    rdd
  }

  private def getKeysNeedCheck(
    rdd: RDD[TiRow]
  ): RDD[ToBeCheckedRow] = {
    //1. check is there any conflicts in data to be inserted
    val cols = tiTableInfo.getColumns.asScala.flatMap { col =>
      if (!col.canSkip(tiTableInfo.isPkHandle)) {
        Some(col)
      } else {
        None
      }
    }

    // TODO: replace get with batch get. It cannot be done by now since batch get api is not being
    //  implemented correctly.
    rdd.map { row =>
      val handleInfo = if (handleCol != null) {
        val handle =
          row.get(handleCol.getOffset, handleCol.getType).asInstanceOf[Number].longValue()
        val handleKey = new SerializableKey(RowKey.toRowKey(tiTableInfo.getId, handle).getBytes)
        val snapshot = TiSessionCache.getSession(tiConf).createSnapshot()
        val oldValue = snapshot.get(handleKey.bytes)
        val oldRow = if (oldValue.nonEmpty) {
          TableCodec.decodeRow(oldValue, cols.toList.asJava)
        } else {
          null
        }
        new keyWithDupInfo(handleKey, oldRow)
      } else {
        null
      }

      var indexKeys: List[keyWithDupInfo] = Nil

      // only do calculation when there is any unique index
      if (uniqueIndices.nonEmpty) {
        indexKeys = uniqueIndices.flatMap {
          val snapshot = TiSessionCache.getSession(tiConf).createSnapshot()
          index =>
            if (index.isUnique) {
              val keys = IndexKey.encodeIndexDataValues(row, index.getIndexColumns, tiTableInfo)
              val indexKey = IndexKey.toIndexKey(tiTableInfo.getId, index.getId, keys: _*)
              val handleVal = snapshot.get(indexKey.getBytes)
              val indexInfo = new keyWithDupInfo(new SerializableKey(indexKey.getBytes), null)
              if (handleVal.nonEmpty) {
                val handle = TableCodec.decodeHandle(handleVal)
                indexInfo.setOldHandle(handle)
              }
              Some(indexInfo)
            } else {
              None
            }
        }
      }
      new ToBeCheckedRow(row, handleInfo, indexKeys)
    }
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

class keyWithDupInfo(var key: SerializableKey, var oldRow: TiRow) extends Serializable {
  var oldHandle: Long = -1L
  def setOldHandle(handle: Long): Unit = this.oldHandle = handle

  override def equals(that: Any): Boolean =
    that match {
      case another: keyWithDupInfo =>
        another.key.equals(this.key)
      case _ => false
    }

  override def hashCode(): Int = {
    util.Arrays.hashCode(key.bytes)
  }
}

class ToBeCheckedRow(val row: TiRow,
                     val handleKey: keyWithDupInfo,
                     val indexKeys: List[keyWithDupInfo])
    extends Serializable {

  //  val idxId: Long
  // if handle is conflict or any key in indices key conflict with each other
  // then two row equals
  override def equals(that: Any): Boolean =
    that match {
      case row: ToBeCheckedRow =>
        if (row == this) return true
        if (indexKeys.isEmpty && row.indexKeys.isEmpty) return false
        val mayConflict = row.indexKeys.toSet
        this.indexKeys.forall(mayConflict)
      case _ =>
        false
    }

  override def hashCode(): Int = {
    var hash = 7
    indexKeys.foreach { kv =>
      hash += util.Arrays.hashCode(kv.key.bytes)
    }
    hash
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
