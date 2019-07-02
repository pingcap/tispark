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
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, TiContext}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

object TiBatchWrite {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def writeToTiDB(df: DataFrame, tiContext: TiContext, options: TiDBOptions): Unit =
    new TiBatchWrite(df, tiContext, options).write()
}

class TiBatchWrite(@transient val df: DataFrame,
                   @transient val tiContext: TiContext,
                   options: TiDBOptions)
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
  private var isEnableSplitRegion: Boolean = _
  private var tableLocked: Boolean = false

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
    isEnableSplitRegion = tiDBJDBCClient.isEnableSplitTable
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
        val start = getAutoTableIdStart(df.count)

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

    // get timestamp as start_ts
    val startTimeStamp = tiSession.getTimestamp

    // for partition table, we need calculate each row and tell which physical table
    // that row is belong to.
    // currently we only support replace and insert.
    val constraintCheckIsNeeded = handleCol != null || uniqueIndices.nonEmpty

    // TODO: region pre split is time consuming we can async execute region pre split operation
    // and generate key-value pairs.  Once the split is successful, we can continue insertion.
    val encodedTiRowRDD = if (constraintCheckIsNeeded) {
      val wrappedRowRdd = if (tiTableInfo.isPkHandle) {
        tiRowRdd.map { row =>
          WrappedRow(row, extractHandleId(row))
        }
      } else {
        val start = getAutoTableIdStart(tiRowRdd.count)
        tiRowRdd.zipWithIndex.map { data =>
          WrappedRow(data._1, data._2 + start)
        }
      }

      val distinctWrappedRowRdd = deduplicate(wrappedRowRdd)

      splitTableRegion(wrappedRowRdd)

      val deletion = generateDataToBeRemovedRdd(distinctWrappedRowRdd, startTimeStamp)
      if (!options.replace && !deletion.isEmpty()) {
        throw new TiBatchWriteException("data to be inserted has conflicts with TiKV data")
      }
      val mergedRDD = generateKV(distinctWrappedRowRdd, remove = false) ++ generateKV(
        deletion,
        remove = true
      )

      mergedRDD.groupByKey().map {
        case (key, iterable) =>
          // if rdd contains same key, it means we need first delete the old value and insert the new value associated the
          // key. We can merge the two operation into one update operation.
          // Note: the deletion operation's value of kv pair is empty.
          val valueOpt = iterable.find(value => value.nonEmpty)
          if (valueOpt.isDefined) {
            (key, valueOpt.get)
          } else {
            (key, new Array[Byte](0))
          }
      }
    } else {
      val start =
        getAutoTableIdStart(tiRowRdd.count)
      val wrappedRowRdd = tiRowRdd.zipWithIndex.map { row =>
        WrappedRow(row._1, row._2 + start)
      }

      splitTableRegion(wrappedRowRdd)
      generateKV(wrappedRowRdd, remove = false)
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

  private def getAutoTableIdStart(step: Long): Long = {
    RowIDAllocator
      .create(
        tiDBInfo.getId,
        tiTableInfo.getId,
        catalog,
        tiTableInfo.isAutoIncColUnsigned,
        step
      )
      .getStart
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

  private def generateDataToBeRemovedRdd(rdd: RDD[WrappedRow], startTs: TiTimestamp) = {
    rdd
      .mapPartitions { wrappedRows =>
        val snapshot = TiSessionCache.getSession(tiConf).createSnapshot(startTs)
        wrappedRows.map { wrappedRow =>
          val rowBuf = mutable.ListBuffer.empty[WrappedRow]
          //  check handle key
          if (handleCol != null) {
            val oldValue = snapshot.get(buildRowKey(wrappedRow.row, wrappedRow.handle).bytes)
            if (oldValue.nonEmpty) {
              val oldRow = TableCodec.decodeRow(oldValue, wrappedRow.handle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, wrappedRow.handle)
            }
          }

          uniqueIndices.foreach { index =>
            val oldValue = snapshot.get(buildUniqueIndexKey(wrappedRow.row, index).bytes)
            if (oldValue.nonEmpty) {
              val oldHandle = TableCodec.decodeHandle(oldValue)
              val oldRowValue = snapshot.get(buildRowKey(wrappedRow.row, oldHandle).bytes)
              val oldRow = TableCodec.decodeRow(
                oldRowValue,
                oldHandle,
                tiTableInfo
              )
              rowBuf += WrappedRow(oldRow, oldHandle)
            }
          }
          rowBuf
        }
      }
      .flatMap(identity)
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

  @throws(classOf[TiBatchWriteException])
  private def deduplicate(rdd: RDD[WrappedRow]) = {
    //1 handle key
    var mutableRdd = rdd
    if (handleCol != null) {
      mutableRdd = mutableRdd
        .map { wrappedRow =>
          val rowKey = buildRowKey(wrappedRow.row, wrappedRow.handle)
          (rowKey, wrappedRow)
        }
        .groupByKey()
        .map(_._2.head)
    }
    uniqueIndices.foreach { index =>
      {
        mutableRdd = mutableRdd
          .map { wrappedRow =>
            val indexKey = buildUniqueIndexKey(wrappedRow.row, index)
            (indexKey, wrappedRow)
          }
          .groupByKey()
          .map(_._2.head)
      }
    }
    mutableRdd
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

  private def extractHandleId(row: TiRow): Long =
    // If handle ID is changed when update, update will remove the old record first,
    // and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    if (tiTableInfo.isPkHandle) {
      row
        .get(handleCol.getOffset, handleCol.getType)
        .asInstanceOf[java.lang.Long]
    } else {
      throw new TiBatchWriteException("cannot extract handle non pk is handle table")
    }

  // convert spark's row to tikv row. We do not allocate handle for no pk case.
  // allocating handle id will be finished after we check conflict.
  private def sparkRow2TiKVRow(sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    val tiRow = ObjectRowImpl.create(fieldCount)
    for (i <- 0 until fieldCount) {
      // TODO: add tiDataType back
      tiRow.set(
        colsMapInTiDB(colsInDf(i)).getOffset,
        null,
        colsMapInTiDB(colsInDf(i)).getType.convertToTiDBType(sparkRow(i))
      )
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

  // construct unique index and non-unique index and value to be inserted into TiKV
  // NOTE:
  //      pk is not handle case is equivalent to unique index.
  //      for non-unique index, handle will be encoded as part of index key. In contrast, unique
  //      index encoded handle to value.
  private def generateUniqueIndexKey(row: TiRow,
                                     handle: Long,
                                     index: TiIndexInfo,
                                     remove: Boolean) = {
    val indexKey = buildUniqueIndexKey(row, index)
    val value = if (remove) {
      new Array[Byte](0)
    } else {
      val cdo = new CodecDataOutput()
      cdo.writeLong(handle)
      cdo.toBytes
    }

    (indexKey, value)
  }

  private def generateSecondaryIndexKey(row: TiRow,
                                        handle: Long,
                                        index: TiIndexInfo,
                                        remove: Boolean) = {
    val keys = IndexKey.encodeIndexDataValues(row, index.getIndexColumns, tiTableInfo)
    val cdo = new CodecDataOutput()
    cdo.write(
      IndexKey.toIndexKey(locatePhysicalTable(row), index.getId, keys: _*).getBytes
    )
    IntegerType.BIGINT.encode(cdo, EncodeType.KEY, handle)
    val value: Array[Byte] = if (remove) {
      new Array[Byte](0)
    } else {
      val value = new Array[Byte](1)
      value(0) = '0'
      value
    }
    (new SerializableKey(cdo.toBytes), value)
  }

  private def buildRowKey(row: TiRow, handle: Long) = {
    new SerializableKey(RowKey.toRowKey(locatePhysicalTable(row), handle).getBytes)
  }

  private def buildUniqueIndexKey(row: TiRow, index: TiIndexInfo) = {
    val keys =
      IndexKey.encodeIndexDataValues(row, index.getIndexColumns, tiTableInfo)
    val indexKey =
      IndexKey.toIndexKey(locatePhysicalTable(row), index.getId, keys: _*)
    new SerializableKey(indexKey.getBytes)
  }

  private def generateRowKey(row: TiRow, handle: Long, remove: Boolean) = {
    if (remove) {
      (
        buildRowKey(row, handle),
        new Array[Byte](0)
      )
    } else {
      (
        new SerializableKey(RowKey.toRowKey(locatePhysicalTable(row), handle).getBytes),
        encodeTiRow(row)
      )
    }
  }

  private def generateKV(rdd: RDD[WrappedRow], remove: Boolean) = {
    rdd
      .map { row =>
        {
          val kvBuf = mutable.ListBuffer.empty[(SerializableKey, Array[Byte])]
          kvBuf += generateRowKey(row.row, row.handle, remove)
          tiTableInfo.getIndices.asScala.foreach { index =>
            if (index.isUnique) {
              kvBuf += generateUniqueIndexKey(row.row, row.handle, index, remove)
            } else {
              kvBuf += generateSecondaryIndexKey(row.row, row.handle, index, remove)
            }
          }
          kvBuf
        }
      }
      .flatMap(identity)
  }

  // TODO: support physical table later. Need use partition info and row value to
  // calculate the real physical table.
  private def locatePhysicalTable(row: TiRow): Long = {
    tiTableInfo.getId
  }

  private def splitTableRegion(wrappedRowRdd: RDD[WrappedRow]) = {
    // when data to be inserted is too small to do region split, we check is user set region split num.
    // If so, we do region split as user's intention. This is also useful for writing test case.
    if (options.enableRegionSplit) {
      if (options.regionSplitNum != 0 && isEnableSplitRegion) {
        tiDBJDBCClient
          .splitTableRegion(
            options.database,
            options.table,
            0,
            Int.MaxValue,
            options.regionSplitNum
          )
      } else {
        val rowSize = tiTableInfo.getEstimatedRowSizeInByte
        val regionSplitNum = (wrappedRowRdd.count() * rowSize) / (96 * 1024 * 1024)
        val minHandle = wrappedRowRdd.min().handle
        val maxHandle = wrappedRowRdd.max().handle
        // region split
        if (regionSplitNum > 1 && isEnableSplitRegion) {
          logger.info("region split is enabled.")
          tiDBJDBCClient
            .splitTableRegion(
              options.database,
              options.table,
              minHandle,
              maxHandle,
              regionSplitNum
            )
        }
      }
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

case class WrappedRow(row: TiRow, handle: Long) extends Ordered[WrappedRow] {
  override def compare(that: WrappedRow): Int = this.handle.toInt - that.handle.toInt
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
