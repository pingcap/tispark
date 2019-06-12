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
import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.codec.{CodecDataOutput, KeyUtils, TableCodec}
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{IndexKey, Key, RowKey}
import com.pingcap.tikv.meta.{TiColumnInfo, TiDBInfo, TiTableInfo, _}
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.types.DataType.EncodeType
import com.pingcap.tikv.types.IntegerType
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer, KeyRangeUtils}
import com.pingcap.tikv.{TiBatchWriteUtils, TiDBJDBCClient, _}
import com.pingcap.tispark.TiBatchWrite.TiRow
import javax.annotation.Nonnull
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, TiContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

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
  @transient private var catalog: Catalog = _

  private var tiTableRef: TiTableReference = _
  private var tiDBInfo: TiDBInfo = _
  private var tiTableInfo: TiTableInfo = _

  private var tableColSize: Int = _

  private var colsMapInTiDB: Map[String, TiColumnInfo] = _

  private var colsInDf: List[String] = _

  private var uniqueIndices: List[TiIndexInfo] = _
  private var handleCol: TiColumnInfo = _

  @transient private var tiDBJDBCClient: TiDBJDBCClient = _
  private var isEnableTableLock: Boolean = _
  private var tableLocked: Boolean = false

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
      .map { handleWithIdx =>
        new SerializableKey(RowKey.toRowKey(tblInfo.getId, handleWithIdx._1).getBytes)
      }
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
    try {
      doWrite()
    } finally {
      close()
    }
  }

  private def close(): Unit = {
    try {
      unlockTable()
    } catch {
      case _: Throwable =>
    }

    try {
      if (tiDBJDBCClient != null) {
        tiDBJDBCClient.close()
      }
    } catch {
      case _: Throwable =>
    }
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  private def doWrite(): Unit = {
    // check if write enable
    if (!tiContext.tiConf.isWriteEnable) {
      throw new TiBatchWriteException(
        "tispark batch write is disabled! set spark.tispark.write.enable to enable."
      )
    }

    // initialize
    tiConf = tiContext.tiConf
    tiSession = tiContext.tiSession
    tiTableRef = options.tiTableRef
    tiDBInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
    tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)
    catalog = TiSessionCache.getSession(tiConf).getCatalog

    if (tiTableInfo == null) {
      throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
    }

    colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
    colsInDf = df.columns.toList
    uniqueIndices = tiTableInfo.getIndices.asScala.filter(index => index.isUnique).toList
    handleCol = tiTableInfo.getPKIsHandleColumn
    tableColSize = tiTableInfo.getColumns.size()

    // check unsupported
    checkUnsupported()

    // check empty
    if (df.count() == 0) {
      logger.warn("data is empty!")
      return
    }

    // lock table
    tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(options.url)())
    isEnableTableLock = tiDBJDBCClient.isEnableTableLock
    lockTable()

    // check schema
    checkColumnNumbers()

    // auto increment
    val rdd = if (tiTableInfo.hasAutoIncrementColumn) {
      val isProvidedID = tableColSize == colsInDf.length
      val autoIncrementColName = tiTableInfo.getAutoIncrementColInfo.getName

      // when auto increment column is provided but the corresponding column in df contains null,
      // we need throw exception
      if (isProvidedID) {
        if (!df.columns.contains(autoIncrementColName)) {
          throw new TiBatchWriteException(
            "Column size is matched but cannot find auto increment column by name"
          )
        }

        val colOffset =
          colsInDf.zipWithIndex.find(col => autoIncrementColName.equals(col._1)).get._2
        colsMapInTiDB(autoIncrementColName).getOffset
        val hasNullValue = df
          .filter(row => row.get(colOffset) == null)
          .count() > 0
        if (hasNullValue) {
          throw new TiBatchWriteException(
            "cannot allocate id on the condition of having null value " +
              "and valid value on auto increment column"
          )
        }
        df.rdd
      } else {
        // if auto increment column is not provided, we need allocate id for it.
        // adding an auto increment column to df
        val newDf = df.withColumn(autoIncrementColName, lit(null).cast("long"))
        val start = RowIDAllocator
          .create(
            tiDBInfo.getId,
            tiTableInfo.getId,
            catalog,
            tiTableInfo.isAutoIncColUnsigned,
            df.count()
          )
          .getStart

        // update colsInDF since we just add one column in df
        colsInDf = newDf.columns.toList
        // last one is auto increment column
        newDf.rdd.zipWithIndex.map { row =>
          val rowSep = row._1.toSeq.zipWithIndex.map { data =>
            val colOffset = data._2
            if (colsMapInTiDB.contains(colsInDf(colOffset))) {
              if (colsMapInTiDB(colsInDf(colOffset)).isAutoIncrement) {
                row._2 + start
              } else {
                data._1
              }
            }
          }
          Row.fromSeq(rowSep)
        }
      }
    } else {
      df.rdd
    }

    // spark row -> tikv row
    val tiRowRdd = rdd.map(row => sparkRow2TiKVRow(row))

    // check value not null
    checkValueNotNull(tiRowRdd)

    // region pre-split
    if (enableRegionPreSplit && handleCol != null) {
      logger.info("region pre split is enabled.")
      val sampleRDD =
        rdd.sample(withReplacement = false, fraction = options.sampleFraction)
      val (dataSize, sampleRDDCount) = estimateDataSize(sampleRDD, options)
      // only perform region presplit if sample rdd is not empty
      if (!sampleRDD.isEmpty()) {
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
    }

    // get timestamp as start_ts
    val startTimeStamp = tiSession.getTimestamp

    // for partition table, we need calculate each row and tell which physical table
    // that row is belong to.
    // currently we only support upsert and insert.
    val toBeCheckedRdd = getKeysNeedCheck(tiRowRdd, startTimeStamp)

    val deduplicatedTiRowRdd = deduplicateIfNecessary(toBeCheckedRdd)
    val rddToBeInserted = if (options.upsert) {
      // step 1: remove duplicate item from data to be inserted. The duplicate basically means
      // two row items violate primary key or unique index constraint.
      deduplicatedTiRowRdd
    } else {
      checkConflictWithInsertKeys(toBeCheckedRdd)
      deduplicatedTiRowRdd
    }

    val encodedTiRowRDD = generateRDDToBeInserted(rddToBeInserted)

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

    val startTs = startTimeStamp.getVersion
    logger.info(s"startTS: $startTs")

    // driver primary pre-write
    val ti2PCClient = new TwoPhaseCommitter(tiConf, startTs)
    val prewritePrimaryBackoff =
      ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, primaryRow)

    // executors secondary pre-write
    finalWriteRDD.foreachPartition { iterator =>
      val ti2PCClientOnExecutor = new TwoPhaseCommitter(tiConf, startTs)

      val pairs = iterator.map {
        case (key, row) =>
          new TwoPhaseCommitter.BytePairWrapper(key.bytes, row)
      }.asJava

      ti2PCClientOnExecutor.prewriteSecondaryKeys(primaryKey.bytes, pairs)

      try {
        ti2PCClientOnExecutor.close()
      } catch {
        case _: Throwable =>
      }
    }

    // driver primary commit
    val commitTs = tiSession.getTimestamp.getVersion
    // check commitTS
    if (commitTs <= startTs) {
      throw new TiBatchWriteException(
        s"invalid transaction tso with startTs=$startTs, commitTs=$commitTs"
      )
    }
    val commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_COMMIT_BACKOFF)

    if (connectionLost()) {
      throw new TiBatchWriteException("tidb's jdbc connection is lost!")
    }
    ti2PCClient.commitPrimaryKey(commitPrimaryBackoff, primaryKey.bytes, commitTs)

    // unlock table
    unlockTable()

    // executors secondary commit
    if (!options.skipCommitSecondaryKey) {
      finalWriteRDD.foreachPartition { iterator =>
        val ti2PCClientOnExecutor = new TwoPhaseCommitter(tiConf, startTs)

        val keys = iterator.map {
          case (key, _) => new TwoPhaseCommitter.ByteWrapper(key.bytes)
        }.asJava

        try {
          ti2PCClientOnExecutor.commitSecondaryKeys(keys, commitTs)
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

  private def lockTable(): Unit = {
    if (isEnableTableLock) {
      if (!tableLocked) {
        tiDBJDBCClient.lockTableWriteLocal(options.database, options.table)
        tableLocked = true
      } else {
        logger.warn("table already locked!")
      }
    } else {
      // TODO: what if version of tidb does not support lock table
    }
  }

  private def unlockTable(): Unit = {
    if (isEnableTableLock) {
      if (tableLocked) {
        tiDBJDBCClient.unlockTables()
        tableLocked = false
      } else {
        logger.warn("table already unlocked!")
      }
    } else {
      // TODO: what if version of tidb does not support lock table
    }
  }

  private def connectionLost(): Boolean = {
    if (isEnableTableLock) {
      tiDBJDBCClient.isClosed
    } else {
      // TODO: what if version of tidb does not support lock table
      false
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
    if (!tiTableInfo.hasAutoIncrementColumn && colsInDf.length != tableColSize) {
      throw new TiBatchWriteException(
        s"table without auto increment column, but data col size ${colsInDf.length} != table column size $tableColSize"
      )
    }

    if (tiTableInfo.hasAutoIncrementColumn && colsInDf.length != tableColSize && colsInDf.length != tableColSize - 1) {
      throw new TiBatchWriteException(
        s"table with auto increment column, but data col size ${colsInDf.length} != table column size $tableColSize and table column size - 1 ${tableColSize - 1} "
      )
    }
  }

  private def checkValueNotNull(rdd: RDD[TiRow]): Unit = {
    val nullRowCount = rdd
      .filter { row =>
        colsMapInTiDB.exists {
          case (_, v) =>
            if (v.getType.isNotNull && row.get(v.getOffset, v.getType) == null) {
              true
            } else {
              false
            }
        }
      }
      .count()

    if (nullRowCount > 0) {
      throw new TiBatchWriteException(
        s"Insert null value to not null column! $nullRowCount rows contain illegal null values!"
      )
    }
  }

  // currently deduplicate can only perform on pk is handle table.
  @throws(classOf[TiBatchWriteException])
  private def deduplicateIfNecessary(rdd: RDD[ToBeCheckedRow]) = {
    val shuffledRDD = rdd.map(row => (row, 0))
    val rddGroupByKey = shuffledRDD.groupByKey
    val duplicateCountRDD = rddGroupByKey.flatMap {
      case (_, iterable) =>
        if (iterable.size > 1) {
          Some(iterable.size)
        } else {
          None
        }
    }

    if (options.deduplicate) {
      rddGroupByKey.map {
        case (row, _) =>
          // remove duplicate rows if key equals
          row
      }
    } else {
      if (!duplicateCountRDD.isEmpty()) {
        throw new TiBatchWriteException("data conflicts! set the parameter deduplicate.")
      }
      rdd
    }
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
      // TODO: add tiDataType back
      tiRow.set(colsMapInTiDB(colsInDf(i)).getOffset, null, sparkRow(i))
    }
    tiRow
  }

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    val colSize = tiRow.fieldCount()

    if (colSize > tableColSize) {
      throw new TiBatchWriteException(
        s"data col size $colSize > table column size $tableColSize"
      )
    }

    // TODO: ddl state change
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-82
    val convertedValues = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      // pk is handle can be skipped
      val columnInfo = tiTableInfo.getColumn(i)
      val value = tiRow.get(i, columnInfo.getType)
      convertedValues.update(i, value)
    }

    TableCodec.encodeRow(tiTableInfo.getColumns, convertedValues, tiTableInfo.isPkHandle)
  }

  private def generateRowKeyToBeInserted(data: ToBeCheckedRow, handle: Long) = {
    (
      new SerializableKey(RowKey.toRowKey(locatePhysicalTable(data.row), handle).getBytes),
      encodeTiRow(data.row)
    )
  }

  // construct unique index and non-unique index and value to be inserted into TiKV
  // NOTE:
  //      pk is not handle case is equivalent to unique index.
  //      for non-unique index, handle will be encoded as part of index key. In contrast, unique
  //      index encoded handle to value.
  private def generateIndexKeyToBeInserted(data: ToBeCheckedRow, handle: Long) = {
    // extract index key from keyWithDupInfo
    val indexKeys = data.indexKeys.map(_.key)
    // unique index  case
    val keyPairsFromUniqueIndex = indexKeys.map { key =>
      val cdo = new CodecDataOutput()
      cdo.writeLong(handle)
      (key, cdo.toBytes)
    }

    val nonUniqueIndices = tiTableInfo.getIndices.asScala.flatMap { index =>
      if (!index.isUnique) {
        Some(index)
      } else {
        None
      }
    }

    // non-unique index case
    keyPairsFromUniqueIndex ++ nonUniqueIndices.map { index =>
      val keys = IndexKey.encodeIndexDataValues(data.row, index.getIndexColumns, tiTableInfo)
      val cdo = new CodecDataOutput()
      cdo.write(
        IndexKey.toIndexKey(locatePhysicalTable(data.row), index.getId, keys: _*).getBytes
      )
      IntegerType.BIGINT.encode(cdo, EncodeType.KEY, handle)
      val zero = new Array[Byte](1)
      zero(0) = '0'
      (new SerializableKey(cdo.toBytes), zero)
    }
  }

  private def generateRowKeyAndNonUniqueIndexKeyToBeRemove(data: ToBeCheckedRow, handle: Long) = {
    val oldHandleRowMap = scala.collection.mutable.Map.empty[Long, TiRow]

    if (data.handleKey != null && data.handleKey.oldHandle != -1) {
      oldHandleRowMap(data.handleKey.oldHandle) = data.handleKey.oldRow
    }

    data.indexKeys.foreach { key =>
      if (key.oldHandle != -1) {
        oldHandleRowMap(key.oldHandle) = key.oldRow
      }
    }
    val nonUniqueIndices = tiTableInfo.getIndices.asScala.flatMap { index =>
      if (!index.isUnique) {
        Some(index)
      } else {
        None
      }
    }

    // according to old handle, we can generate non-unique index key to be removed.
    val nonUniqueKeysToBeRemoved = oldHandleRowMap.map {
      case (oldHandle, row) =>
        nonUniqueIndices.map { index =>
          val keys =
            IndexKey.encodeIndexDataValues(
              row,
              index.getIndexColumns,
              tiTableInfo
            )
          val cdo = new CodecDataOutput()
          cdo.write(
            IndexKey
              .toIndexKey(locatePhysicalTable(row), index.getId, keys: _*)
              .getBytes
          )
          IntegerType.BIGINT.encode(cdo, EncodeType.KEY, oldHandle)
          (new SerializableKey(cdo.toBytes), new Array[Byte](0))
        }
    }.flatten

    // according to old handle, we can generate row key to be removed.
    val rowKeyToBeRemoved = oldHandleRowMap.map {
      case (oldHandle, row) =>
        (
          new SerializableKey(
            RowKey.toRowKey(locatePhysicalTable(row), oldHandle).getBytes
          ),
          new Array[Byte](0)
        )
    }.toList
    rowKeyToBeRemoved ++ nonUniqueKeysToBeRemoved
  }

  // since we only need generate unique index key on these unique index is not conflict with data in
  // TiKV, so we need handle this logic separately.
  private def generateIndexKeyToBeRemoved(data: ToBeCheckedRow, handle: Long) = {
    // unique index keys
    val keyPairsFromUniqueIndex = data.indexKeys.flatMap { key =>
      {
        // only remove unique index is not conflicted since the conflict index will be updated
        if (key.oldHandle != -1) {
          uniqueIndices.flatMap { index =>
            if (index.getId != key.indexId) {
              val keys =
                IndexKey.encodeIndexDataValues(key.oldRow, index.getIndexColumns, tiTableInfo)
              val indexKey =
                IndexKey.toIndexKey(locatePhysicalTable(key.oldRow), index.getId, keys: _*)
              Some(new SerializableKey(indexKey.getBytes), new Array[Byte](0))
            } else {
              None
            }
          }
        } else {
          None
        }
      }
    }

    // handle keys
    val keyPairsFromHandleKey = if (data.handleKey != null) {
      if (data.handleKey.oldRow != null) {
        val oldRow = data.handleKey.oldRow
        uniqueIndices.flatMap { index =>
          val keys =
            IndexKey.encodeIndexDataValues(oldRow, index.getIndexColumns, tiTableInfo)
          val indexKey =
            IndexKey.toIndexKey(locatePhysicalTable(oldRow), index.getId, keys: _*)
          Some(new SerializableKey(indexKey.getBytes), new Array[Byte](0))
        }
      } else {
        Nil
      }
    } else {
      Nil
    }

    keyPairsFromUniqueIndex ++ keyPairsFromHandleKey
  }

  private def generateRDDToBeInserted(
    rdd: RDD[ToBeCheckedRow]
  ) = {
    var allocatedRdd: RDD[(ToBeCheckedRow, Long)] = null
    if (!tiTableInfo.isPkHandle) {
      val step = rdd.count
      // start means current largest allocated id in TiKV with specific table.
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

    // 1. construct row key and value to be inserted into TiKV
    allocatedRdd
      .map {
        case (data, handle) =>
          val keysBuf = ListBuffer(generateRowKeyToBeInserted(data, handle))
          keysBuf ++= generateIndexKeyToBeInserted(data, handle)
          keysBuf ++= generateIndexKeyToBeRemoved(data, handle)
          keysBuf ++= generateRowKeyAndNonUniqueIndexKeyToBeRemove(data, handle)
          keysBuf
      }
      .flatMap(identity)
  }

  private def checkConflictWithInsertKeys(
    rdd: RDD[ToBeCheckedRow]
  ): Unit = {
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

      var handleDuplicated = false
      rdd
        .map { row =>
          (row.handleKey, 0)
        }
        .reduceByKey { (x, _) =>
          handleDuplicated = true
          x
        }

      if (handleDuplicated) {
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
        .isEmpty()

      if (!indexConflict) {
        throw new TiBatchWriteException("data to be inserted is conflict on unique index")
      }
    }
  }

  // TODO: support physical table later. Need use partition info and row value to
  // calculate the real physical table.
  private def locatePhysicalTable(row: TiRow): Long = {
    tiTableInfo.getId
  }

  private def getKeysNeedCheck(
    rdd: RDD[TiRow],
    startTs: TiTimestamp
  ): RDD[ToBeCheckedRow] = {
    // TODO: replace get with batch get. It cannot be done by now since batch get api is not being
    //  implemented correctly.

    rdd.map { row =>
      val physicalTableId = locatePhysicalTable(row)
      val handleInfo = if (handleCol != null) {
        val handle =
          row.get(handleCol.getOffset, handleCol.getType).asInstanceOf[Number].longValue()
        val handleKey = new SerializableKey(RowKey.toRowKey(physicalTableId, handle).getBytes)
        val snapshot = TiSessionCache.getSession(tiConf).createSnapshot(startTs)
        val oldValue = snapshot.get(handleKey.bytes)
        val oldRow = if (oldValue.nonEmpty) {
          TableCodec.decodeRow(oldValue, handle, tiTableInfo)
        } else {
          null
        }
        new KeyWithDupInfo(handleKey, oldRow)
      } else {
        null
      }

      var indexKeys: List[KeyWithDupInfo] = Nil

      // only do calculation when there is any unique index
      if (uniqueIndices.nonEmpty) {
        indexKeys = uniqueIndices.flatMap {
          val snapshot = TiSessionCache.getSession(tiConf).createSnapshot(startTs)
          index =>
            if (index.isUnique) {
              val keys = IndexKey.encodeIndexDataValues(row, index.getIndexColumns, tiTableInfo)
              val indexKey = IndexKey.toIndexKey(physicalTableId, index.getId, keys: _*)
              val handleVal = snapshot.get(indexKey.getBytes)
              val indexInfo = new KeyWithDupInfo(
                new SerializableKey(indexKey.getBytes),
                null,
                indexId = index.getId
              )
              if (handleVal.nonEmpty) {
                val handle = TableCodec.decodeHandle(handleVal)
                indexInfo.setOldHandle(handle)
                tiTableInfo.isPkHandle
                val oldRow = TableCodec.decodeRow(
                  snapshot.get(RowKey.toRowKey(physicalTableId, handle).getBytes),
                  handle,
                  tiTableInfo
                )
                indexInfo.setOldRow(oldRow)
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

class KeyWithDupInfo(@Nonnull val key: SerializableKey,
                     var oldRow: TiRow,
                     var oldHandle: Long = -1L,
                     val indexId: Long = -1)
    extends Serializable {
  def setOldHandle(handle: Long): Unit = this.oldHandle = handle

  def setOldRow(oldRow: TiRow): Unit = this.oldRow = oldRow

  override def equals(that: Any): Boolean =
    that match {
      case another: KeyWithDupInfo =>
        another.key.equals(this.key)
      case _ => false
    }

  override def hashCode(): Int = {
    util.Arrays.hashCode(key.bytes)
  }
}

// Note: This object's hashCode return a fixed value which suggests it is not better use with
// hash map, hash set, or any other places that use hashCode. It is only used in reduceByKey.
class ToBeCheckedRow(val row: TiRow,
                     val handleKey: KeyWithDupInfo,
                     val indexKeys: List[KeyWithDupInfo])
    extends Serializable {
  // When one of the following case happens, two ToBeCheckedRows are considered as same:
  // 1. handle key is same if not null
  // 2. at least one of index keys is same and index keys is not empty
  // 3. handle key is null and index keys are empty, but row is same.
  override def equals(that: Any): Boolean =
    that match {
      case row: ToBeCheckedRow =>
        // 1. handle key is same
        var handleConflict = false
        if (handleKey != null && row.handleKey != null) {
          handleConflict = handleKey.equals(row.handleKey)
        }
        // 2. index keys are empty, just return false
        if (indexKeys.isEmpty && row.indexKeys.isEmpty) {
          return handleConflict
        }
        // 3. check is there any index key same
        this.indexKeys.exists(row.indexKeys.toSet) || handleConflict
      case _ =>
        false
    }

  // there is no way to implement hashCode correctly without breaking the contract of hashCode.
  override def hashCode(): Int = {
    7
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
