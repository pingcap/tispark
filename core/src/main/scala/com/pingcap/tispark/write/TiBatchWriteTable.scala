/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tispark.write

import java.sql.SQLException
import java.util

import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.codec.{CodecDataOutput, TableCodec}
import com.pingcap.tikv.exception.{
  ConvertOverflowException,
  TiBatchWriteException,
  TiDBConvertException
}
import com.pingcap.tikv.key.{IndexKey, RowKey}
import com.pingcap.tikv.meta._
import com.pingcap.tikv.region.TiRegion
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.types.DataType.EncodeType
import com.pingcap.tikv.types.IntegerType
import com.pingcap.tikv.util.FastByteComparisons
import com.pingcap.tikv.{
  BytePairWrapper,
  TiBatchWriteUtils,
  TiConfiguration,
  TiDBJDBCClient,
  TiSession
}
import com.pingcap.tispark.TiTableReference
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{TiContext, _}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

class TiBatchWriteTable(
    @transient var df: DataFrame,
    @transient val tiContext: TiContext,
    val options: TiDBOptions,
    val tiConf: TiConfiguration,
    @transient val tiDBJDBCClient: TiDBJDBCClient,
    val isEnableSplitRegion: Boolean)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._
  @transient private val tiSession = tiContext.tiSession
  // only fetch row format version once for each batch write process
  private val enableNewRowFormat: Boolean = tiDBJDBCClient.getRowFormatVersion == 2
  private var tiTableRef: TiTableReference = _
  private var tiDBInfo: TiDBInfo = _
  private var tiTableInfo: TiTableInfo = _
  private var tableColSize: Int = _
  private var colsMapInTiDB: Map[String, TiColumnInfo] = _
  private var colsInDf: List[String] = _
  private var uniqueIndices: Seq[TiIndexInfo] = _
  private var handleCol: TiColumnInfo = _
  private var tableLocked: Boolean = false

  tiTableRef = options.getTiTableRef(tiConf)
  tiDBInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
  tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)

  if (tiTableInfo == null) {
    throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
  }

  colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
  colsInDf = df.columns.toList.map(_.toLowerCase())
  uniqueIndices = tiTableInfo.getIndices.asScala.filter(index => index.isUnique)
  handleCol = tiTableInfo.getPKIsHandleColumn
  tableColSize = tiTableInfo.getColumns.size()

  def persist(): Unit = {
    df = df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
  }

  def isDFEmpty: Boolean = {
    if (TiUtil.isDataFrameEmpty(df.select(df.columns.head))) {
      logger.warn(s"the dataframe write to $tiTableRef is empty!")
      true
    } else {
      false
    }
  }

  def checkSchemaChange(): Unit = {
    val newTableInfo =
      tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)
    if (tiTableInfo.getUpdateTimestamp < newTableInfo.getUpdateTimestamp) {
      throw new TiBatchWriteException("schema has changed during prewrite!")
    }
  }

  def preCalculate(startTimeStamp: TiTimestamp): RDD[(SerializableKey, Array[Byte])] = {
    val sc = tiContext.sparkSession.sparkContext

    df.explain(true)

    val count = df.count
    logger.info(s"source data count=$count")

    // auto increment
    val rdd = if (tiTableInfo.hasAutoIncrementColumn) {
      val isProvidedID = tableColSize == colsInDf.length
      val autoIncrementColName = tiTableInfo.getAutoIncrementColInfo.getName

      // when auto increment column is provided but the corresponding column in df contains null,
      // we need throw exception
      if (isProvidedID) {
        throw new TiBatchWriteException(
          "currently user provided auto increment value is not supported!")
        /*if (!df.columns.contains(autoIncrementColName)) {
          throw new TiBatchWriteException(
            "Column size is matched but cannot find auto increment column by name")
        }

        val hasNullValue = !df
          .select(autoIncrementColName)
          .filter(row => row.get(0) == null)
          .rdd
          .isEmpty()
        if (hasNullValue) {
          throw new TiBatchWriteException(
            "cannot allocate id on the condition of having null value and valid value on auto increment column")
        }
        df.rdd*/
      } else {
        // if auto increment column is not provided, we need allocate id for it.
        // adding an auto increment column to df
        val newDf = df.withColumn(autoIncrementColName, lit(null).cast("long"))
        val start = getAutoTableIdStart(count)

        // update colsInDF since we just add one column in df
        colsInDf = newDf.columns.toList.map(_.toLowerCase())
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

    // for partition table, we need calculate each row and tell which physical table
    // that row is belong to.
    // currently we only support replace and insert.
    val constraintCheckIsNeeded = handleCol != null || uniqueIndices.nonEmpty

    val keyValueRDD = if (constraintCheckIsNeeded) {
      val wrappedRowRdd = if (tiTableInfo.isPkHandle) {
        tiRowRdd.map { row =>
          WrappedRow(row, extractHandleId(row))
        }
      } else {
        val start = getAutoTableIdStart(count)
        tiRowRdd.zipWithIndex.map { data =>
          WrappedRow(data._1, data._2 + start)
        }
      }

      val distinctWrappedRowRdd = deduplicate(wrappedRowRdd)

      val deletion = (if (options.useSnapshotBatchGet) {
                        generateDataToBeRemovedRddV2(distinctWrappedRowRdd, startTimeStamp)
                      } else {
                        generateDataToBeRemovedRddV1(distinctWrappedRowRdd, startTimeStamp)
                      }).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)

      if (!options.replace && !deletion.isEmpty()) {
        throw new TiBatchWriteException("data to be inserted has conflicts with TiKV data")
      }

      val wrappedEncodedRecordRdd = generateRecordKV(distinctWrappedRowRdd, remove = false)
      val wrappedEncodedIndexRdds = generateIndexKVs(distinctWrappedRowRdd, remove = false)
      val wrappedEncodedIndexRdd: RDD[WrappedEncodedRow] = {
        val list = wrappedEncodedIndexRdds.values.toSeq
        if (list.isEmpty) {
          sc.emptyRDD[WrappedEncodedRow]
        } else if (list.lengthCompare(1) == 0) {
          list.head
        } else {
          sc.union(list)
        }
      }

      if ("v1".equals(options.regionSplitMethod)) {
        splitTableRegion(wrappedEncodedRecordRdd)
        splitIndexRegion(wrappedEncodedIndexRdds, count)
      }

      val g1 = (wrappedEncodedRecordRdd ++ generateRecordKV(deletion, remove = true))
        .map(wrappedEncodedRow => (wrappedEncodedRow.encodedKey, wrappedEncodedRow))
        .reduceByKey { (r1, r2) =>
          // if rdd contains same key, it means we need first delete the old value and insert the new value associated the
          // key. We can merge the two operation into one update operation.
          // Note: the deletion operation's value of kv pair is empty.
          if (r1.encodedValue.isEmpty) r2 else r1
        }
        .map(_._2)
      val g2 = (wrappedEncodedIndexRdd ++ generateIndexKV(sc, deletion, remove = true))
        .map(wrappedEncodedRow => (wrappedEncodedRow.encodedKey, wrappedEncodedRow))
        .reduceByKey { (r1, r2) =>
          if (r1.encodedValue.isEmpty) r2 else r1
        }
        .map(_._2)

      (g1 ++ g2).map(obj => (obj.encodedKey, obj.encodedValue))
    } else {
      val start = getAutoTableIdStart(count)
      val wrappedRowRdd = tiRowRdd.zipWithIndex.map { row =>
        WrappedRow(row._1, row._2 + start)
      }

      val wrappedEncodedRecordRdd = generateRecordKV(wrappedRowRdd, remove = false)
      val wrappedEncodedIndexRdds = generateIndexKVs(wrappedRowRdd, remove = false)
      val wrappedEncodedIndexRdd = sc.union(wrappedEncodedIndexRdds.values.toSeq)

      if ("v1".equals(options.regionSplitMethod)) {
        splitTableRegion(wrappedEncodedRecordRdd)
        splitIndexRegion(wrappedEncodedIndexRdds, count)
      }

      (wrappedEncodedRecordRdd ++ wrappedEncodedIndexRdd).map(obj =>
        (obj.encodedKey, obj.encodedValue))
    }

    // shuffle or persist
    val shuffledOrPersistedRDD =
      if (options.enableRegionSplit && "v2".equals(options.regionSplitMethod)) {
        keyValueRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      } else if (options.shuffleKeyToSameRegion) {
        shuffleKeyToSameRegion(keyValueRDD)
      } else {
        shuffleUseRepartition(keyValueRDD)
      }
    shuffledOrPersistedRDD
  }

  def lockTable(): Unit = {
    if (!tableLocked) {
      tiDBJDBCClient.lockTableWriteLocal(options.database, options.table)
      tableLocked = true
    } else {
      logger.warn("table already locked!")
    }
  }

  def unlockTable(): Unit = {
    if (tableLocked) {
      tiDBJDBCClient.unlockTables()
      tableLocked = false
    }
  }

  def checkUnsupported(): Unit = {
    // write to partition table
    if (tiTableInfo.isPartitionEnabled) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to partition table!")
    }

    // write to table with generated column
    if (tiTableInfo.hasGeneratedColumn) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to table with generated column!")
    }
  }

  def checkColumnNumbers(): Unit = {
    if (!tiTableInfo.hasAutoIncrementColumn && colsInDf.lengthCompare(tableColSize) != 0) {
      throw new TiBatchWriteException(
        s"table without auto increment column, but data col size ${colsInDf.length} != table column size $tableColSize")
    }

    if (tiTableInfo.hasAutoIncrementColumn && colsInDf.lengthCompare(
        tableColSize) != 0 && colsInDf.lengthCompare(tableColSize - 1) != 0) {
      throw new TiBatchWriteException(
        s"table with auto increment column, but data col size ${colsInDf.length} != table column size $tableColSize and table column size - 1 ${tableColSize - 1} ")
    }
  }

  private def getAutoTableIdStart(step: Long): Long = {
    RowIDAllocator
      .create(tiDBInfo.getId, tiTableInfo, tiConf, tiTableInfo.isAutoIncColUnsigned, step)
      .getStart
  }

  private def isNullUniqueIndexValue(value: Array[Byte]): Boolean = {
    value.length == 1 && value(0) == '0'
  }

  private def generateDataToBeRemovedRddV1(
      rdd: RDD[WrappedRow],
      startTs: TiTimestamp): RDD[WrappedRow] = {
    rdd
      .mapPartitions { wrappedRows =>
        val snapshot = TiSession.getInstance(tiConf).createSnapshot(startTs.getPrevious)
        wrappedRows.map { wrappedRow =>
          val rowBuf = mutable.ListBuffer.empty[WrappedRow]
          //  check handle key
          if (handleCol != null) {
            val oldValue = snapshot.get(buildRowKey(wrappedRow.row, wrappedRow.handle).bytes)
            if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
              val oldRow = TableCodec.decodeRow(oldValue, wrappedRow.handle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, wrappedRow.handle)
            }
          }

          uniqueIndices.foreach { index =>
            val keyInfo = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)
            // if handle is appended, it must not exists in old table
            if (!keyInfo._2) {
              val oldValue = snapshot.get(keyInfo._1.bytes)
              if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
                val oldHandle = TableCodec.decodeHandle(oldValue)
                val oldRowValue = snapshot.get(buildRowKey(wrappedRow.row, oldHandle).bytes)
                val oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo)
                rowBuf += WrappedRow(oldRow, oldHandle)
              }
            }
          }
          rowBuf
        }
      }
      .flatMap(identity)
  }

  private def generateDataToBeRemovedRddV2(
      rdd: RDD[WrappedRow],
      startTs: TiTimestamp): RDD[WrappedRow] = {
    rdd.mapPartitions { wrappedRows =>
      val snapshot = TiSession.getInstance(tiConf).createSnapshot(startTs.getPrevious)
      var rowBuf = mutable.ListBuffer.empty[WrappedRow]
      var rowBufIterator = rowBuf.iterator

      new Iterator[WrappedRow] {
        override def hasNext: Boolean = {
          while (true) {
            if (!wrappedRows.hasNext && !rowBufIterator.hasNext) {
              return false
            }

            if (rowBufIterator.hasNext) {
              return true
            }

            processNextBatch()
          }
          assert(false)
          false
        }

        override def next(): WrappedRow = {
          if (hasNext) {
            rowBufIterator.next
          } else {
            null
          }
        }

        def getNextBatch(itor: Iterator[WrappedRow]): List[WrappedRow] = {
          val buf = mutable.ListBuffer.empty[WrappedRow]
          var i = 0
          while (itor.hasNext && i < options.snapshotBatchGetSize) {
            buf.append(itor.next())
            i = i + 1
          }
          buf.toList
        }

        def genNextHandleBatch(
            batch: List[WrappedRow]): (util.List[Array[Byte]], util.List[Long]) = {
          val list = new util.ArrayList[Array[Byte]]()
          val handles = new util.ArrayList[Long]()
          batch.foreach { wrappedRow =>
            val bytes = buildRowKey(wrappedRow.row, wrappedRow.handle).bytes
            list.add(bytes)
            handles.add(wrappedRow.handle)
          }
          (list, handles)
        }

        def genNextUniqueIndexBatch(
            batch: List[WrappedRow],
            index: TiIndexInfo): (util.List[Array[Byte]], util.List[TiRow]) = {
          val keyList = new util.ArrayList[Array[Byte]]()
          val rowList = new util.ArrayList[TiRow]()
          batch.foreach { wrappedRow =>
            val encodeResult = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)
            if (!encodeResult._2) {
              // only add the key if handle is not appended, since if handle is appened,
              // the value must be a new value
              val bytes = encodeResult._1.bytes
              keyList.add(bytes)
              rowList.add(wrappedRow.row)
            }
          }
          (keyList, rowList)
        }

        def decodeHandle(row: Array[Byte]): Long = {
          RowKey.decode(row).getHandle
        }

        def processHandleDelete(
            oldValueList: java.util.List[BytePairWrapper],
            handleList: java.util.List[Long]): Unit = {
          for (i <- 0 until oldValueList.size) {
            val oldValuePair = oldValueList.get(i)
            val oldValue = oldValuePair.getValue
            val handle = handleList.get(i)

            if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
              val oldRow = TableCodec.decodeRow(oldValue, handle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, handle)
            }
          }
        }

        def processNextBatch(): Unit = {
          rowBuf = mutable.ListBuffer.empty[WrappedRow]

          val batch = getNextBatch(wrappedRows)

          if (handleCol != null) {
            val (batchHandle, handleList) = genNextHandleBatch(batch)
            val oldValueList = snapshot.batchGet(options.batchGetBackOfferMS, batchHandle)
            processHandleDelete(oldValueList, handleList)
          }

          val oldIndicesBatch: util.List[Array[Byte]] = new util.ArrayList[Array[Byte]]()
          uniqueIndices.foreach { index =>
            val (batchIndices, rowList) = genNextUniqueIndexBatch(batch, index)
            val oldValueList = snapshot.batchGet(options.batchGetBackOfferMS, batchIndices)
            for (i <- 0 until oldValueList.size) {
              val oldValuePair = oldValueList.get(i)
              val oldValue = oldValuePair.getValue
              if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
                val oldHandle = TableCodec.decodeHandle(oldValue)
                val tiRow = rowList.get(i)

                oldIndicesBatch.add(buildRowKey(tiRow, oldHandle).bytes)
              }
            }
          }

          val oldIndicesRowPairs = snapshot.batchGet(options.batchGetBackOfferMS, oldIndicesBatch)
          oldIndicesRowPairs.asScala.foreach { oldIndicesRowPair =>
            val oldRowKey = oldIndicesRowPair.getKey
            val oldRowValue = oldIndicesRowPair.getValue
            if (oldRowValue.nonEmpty && !isNullUniqueIndexValue(oldRowValue)) {
              val oldHandle = decodeHandle(oldRowKey)
              val oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, oldHandle)
            }
          }

          rowBufIterator = rowBuf.iterator
        }
      }
    }
  }

  private def checkValueNotNull(rdd: RDD[TiRow]): Unit = {
    val nullRows = !rdd
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
      .isEmpty()

    if (nullRows) {
      throw new TiBatchWriteException(
        s"Insert null value to not null column! rows contain illegal null values!")
    }
  }

  @throws(classOf[TiBatchWriteException])
  private def deduplicate(rdd: RDD[WrappedRow]): RDD[WrappedRow] = {
    //1 handle key
    var mutableRdd = rdd
    if (handleCol != null) {
      mutableRdd = mutableRdd
        .map { wrappedRow =>
          val rowKey = buildRowKey(wrappedRow.row, wrappedRow.handle)
          (rowKey, wrappedRow)
        }
        .reduceByKey((r1, _) => r1)
        .map(_._2)
    }
    uniqueIndices.foreach { index =>
      {
        mutableRdd = mutableRdd
          .map { wrappedRow =>
            val indexKey = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)._1
            (indexKey, wrappedRow)
          }
          .reduceByKey((r1, _) => r1)
          .map(_._2)
      }
    }
    mutableRdd
  }

  @throws(classOf[NoSuchTableException])
  private def shuffleKeyToSameRegion(
      rdd: RDD[(SerializableKey, Array[Byte])]): RDD[(SerializableKey, Array[Byte])] = {
    val regions = getRegions
    assert(regions.size() > 0)
    val tiRegionPartitioner =
      new TiRegionPartitioner(regions, options.writeConcurrency, options.taskNumPerRegion)

    rdd.partitionBy(tiRegionPartitioner)
  }

  @throws(classOf[NoSuchTableException])
  private def shuffleUseRepartition(
      rdd: RDD[(SerializableKey, Array[Byte])]): RDD[(SerializableKey, Array[Byte])] = {
    if (options.writeTaskNumber > 0) {
      rdd.repartition(options.writeTaskNumber)
    } else {
      rdd
    }
  }

  private def getRegions: util.List[TiRegion] = {
    val regions = TiBatchWriteUtils.getRegionsByTable(tiSession, tiTableInfo)
    logger.info(s"find ${regions.size} regions in $tiTableRef tableId: ${tiTableInfo.getId}")
    regions
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
      try {
        tiRow.set(
          colsMapInTiDB(colsInDf(i)).getOffset,
          null,
          colsMapInTiDB(colsInDf(i)).getType.convertToTiDBType(sparkRow(i)))
      } catch {
        case e: ConvertOverflowException =>
          throw new ConvertOverflowException(
            e.getMessage,
            new TiDBConvertException(colsMapInTiDB(colsInDf(i)).getName, e))
        case e: Throwable =>
          throw new TiDBConvertException(colsMapInTiDB(colsInDf(i)).getName, e)
      }
    }
    tiRow
  }

  @throws(classOf[TiBatchWriteException])
  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    val colSize = tiRow.fieldCount()

    if (colSize > tableColSize) {
      throw new TiBatchWriteException(s"data col size $colSize > table column size $tableColSize")
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

    TableCodec.encodeRow(
      tiTableInfo.getColumns,
      convertedValues,
      tiTableInfo.isPkHandle,
      enableNewRowFormat)
  }

  // construct unique index and non-unique index and value to be inserted into TiKV
  // NOTE:
  //      pk is not handle case is equivalent to unique index.
  //      for non-unique index, handle will be encoded as part of index key. In contrast, unique
  //      index encoded handle to value.
  private def generateUniqueIndexKey(
      row: TiRow,
      handle: Long,
      index: TiIndexInfo,
      remove: Boolean): (SerializableKey, Array[Byte]) = {
    val encodeResult = buildUniqueIndexKey(row, handle, index)
    val indexKey = encodeResult._1
    val value = if (remove) {
      new Array[Byte](0)
    } else {
      if (encodeResult._2) {
        val value = new Array[Byte](1)
        value(0) = '0'
        value
      } else {
        val cdo = new CodecDataOutput()
        cdo.writeLong(handle)
        cdo.toBytes
      }
    }

    (indexKey, value)
  }

  private def generateSecondaryIndexKey(
      row: TiRow,
      handle: Long,
      index: TiIndexInfo,
      remove: Boolean): (SerializableKey, Array[Byte]) = {
    val keys =
      IndexKey.encodeIndexDataValues(row, index.getIndexColumns, handle, false, tiTableInfo).keys
    val cdo = new CodecDataOutput()
    cdo.write(IndexKey.toIndexKey(locatePhysicalTable(row), index.getId, keys: _*).getBytes)
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

  private def buildRowKey(row: TiRow, handle: Long): SerializableKey = {
    new SerializableKey(RowKey.toRowKey(locatePhysicalTable(row), handle).getBytes)
  }

  private def buildUniqueIndexKey(
      row: TiRow,
      handle: Long,
      index: TiIndexInfo): (SerializableKey, Boolean) = {
    // NULL is only allowed in unique key, primary key does not allow NULL value
    val encodeResult = IndexKey.encodeIndexDataValues(
      row,
      index.getIndexColumns,
      handle,
      index.isUnique && !index.isPrimary,
      tiTableInfo)
    val keys = encodeResult.keys
    val indexKey =
      IndexKey.toIndexKey(locatePhysicalTable(row), index.getId, keys: _*)
    (new SerializableKey(indexKey.getBytes), encodeResult.appendHandle)
  }

  private def generateRowKey(
      row: TiRow,
      handle: Long,
      remove: Boolean): (SerializableKey, Array[Byte]) = {
    if (remove) {
      (buildRowKey(row, handle), new Array[Byte](0))
    } else {
      (
        new SerializableKey(RowKey.toRowKey(locatePhysicalTable(row), handle).getBytes),
        encodeTiRow(row))
    }
  }

  private def calcSize(rdd: RDD[WrappedEncodedRow]): Long = {
    rdd.aggregate(0L)(
      (prev, r) => prev + r.encodedKey.bytes.length + r.encodedValue.length,
      (r1, r2) => r1 + r2)
  }

  private def calcRecordMinMax(rdd: RDD[WrappedEncodedRow]): (Long, Long) = {
    rdd.aggregate((Long.MaxValue, Long.MinValue))(
      (prev, r) => (Math.min(prev._1, r.handle), Math.max(prev._2, r.handle)),
      (r1, r2) => (Math.min(r1._1, r2._1), Math.max(r1._2, r2._2)))
  }

  private def calcIndexMinMax(rdd: RDD[WrappedEncodedRow], index: TiIndexInfo): (Any, Any) = {
    val colName = index.getIndexColumns.get(0).getName
    val tiColumn = tiTableInfo.getColumn(colName)
    val colOffset = tiColumn.getOffset
    val dataType = tiColumn.getType

    def compare(x: Any, y: Any): Int = {
      x match {
        case _: java.lang.Integer =>
          x.asInstanceOf[java.lang.Integer]
            .compareTo(y.asInstanceOf[java.lang.Integer])
        case _: java.lang.Long =>
          x.asInstanceOf[java.lang.Long]
            .compareTo(y.asInstanceOf[java.lang.Long])
        case _: java.lang.Double =>
          x.asInstanceOf[java.lang.Double]
            .compareTo(y.asInstanceOf[java.lang.Double])
        case _: java.lang.Float =>
          x.asInstanceOf[java.lang.Float]
            .compareTo(y.asInstanceOf[java.lang.Float])
        case _: Array[Byte] =>
          FastByteComparisons.compareTo(x.asInstanceOf[Array[Byte]], y.asInstanceOf[Array[Byte]])
        case _ => x.toString.compareTo(y.toString)
      }
    }

    def min(x: Any, y: Any): Any = {
      if (x == null) y
      else if (y == null) x
      else if (compare(x, y) < 0) x
      else y
    }

    def max(x: Any, y: Any): Any = {
      if (x == null) y
      else if (y == null) x
      else if (compare(x, y) > 0) x
      else y
    }

    rdd.aggregate((null: Any, null: Any))(
      (prev, r) => {
        val v = r.row.get(colOffset, dataType)
        (min(prev._1, v), max(prev._2, v))
      },
      (r1, r2) => (min(r1._1, r2._1), max(r1._2, r2._2)))
  }

  private def generateRecordKV(rdd: RDD[WrappedRow], remove: Boolean): RDD[WrappedEncodedRow] = {
    rdd
      .map { row =>
        {
          val (encodedKey, encodedValue) = generateRowKey(row.row, row.handle, remove)
          WrappedEncodedRow(
            row.row,
            row.handle,
            encodedKey,
            encodedValue,
            isIndex = false,
            -1,
            remove)
        }
      }
  }

  private def generateIndexRDD(
      rdd: RDD[WrappedRow],
      index: TiIndexInfo,
      remove: Boolean): RDD[WrappedEncodedRow] = {
    if (index.isUnique) {
      rdd.map { row =>
        val (encodedKey, encodedValue) =
          generateUniqueIndexKey(row.row, row.handle, index, remove)
        WrappedEncodedRow(
          row.row,
          row.handle,
          encodedKey,
          encodedValue,
          isIndex = true,
          index.getId,
          remove)
      }
    } else {
      rdd.map { row =>
        val (encodedKey, encodedValue) =
          generateSecondaryIndexKey(row.row, row.handle, index, remove)
        WrappedEncodedRow(
          row.row,
          row.handle,
          encodedKey,
          encodedValue,
          isIndex = true,
          index.getId,
          remove)
      }
    }
  }

  private def generateIndexKVs(
      rdd: RDD[WrappedRow],
      remove: Boolean): Map[Long, RDD[WrappedEncodedRow]] = {
    tiTableInfo.getIndices.asScala
      .map(index => (index.getId, generateIndexRDD(rdd, index, remove)))
      .toMap
  }

  private def unionAll(
      sc: SparkContext,
      rdds: Map[Long, RDD[WrappedEncodedRow]]): RDD[WrappedEncodedRow] = {
    rdds.values.foldLeft(sc.emptyRDD[WrappedEncodedRow])(_ ++ _)
  }

  private def generateIndexKV(
      sc: SparkContext,
      rdd: RDD[WrappedRow],
      remove: Boolean): RDD[WrappedEncodedRow] = {
    unionAll(sc, generateIndexKVs(rdd, remove))
  }

  // TODO: support physical table later. Need use partition info and row value to
  // calculate the real physical table.
  private def locatePhysicalTable(row: TiRow): Long = {
    tiTableInfo.getId
  }

  private def estimateRegionSplitNum(totalSize: Long): Long = {
    //TODO: replace 96 with actual value read from pd https://github.com/pingcap/tispark/issues/890
    Math.ceil(totalSize / (tiContext.tiConf.getTikvRegionSplitSizeInMB * 1024.0 * 1024)).toLong
  }

  private def checkTidbRegionSplitContidion(
      minHandle: Long,
      maxHandle: Long,
      regionSplitNum: Long): Boolean = {
    maxHandle - minHandle > regionSplitNum * 1000
  }

  private def toString(value: Any): String = {
    value match {
      case a: Array[Byte] => java.util.Arrays.toString(a)
      case _ => value.toString
    }
  }

  private def splitIndexRegion(
      wrappedEncodedRdd: Map[Long, RDD[WrappedEncodedRow]],
      count: Long): Unit = {
    if (options.enableRegionSplit && isEnableSplitRegion) {
      val indices = tiTableInfo.getIndices.asScala

      indices.foreach { index =>
        val colName = index.getIndexColumns.get(0).getName
        val tiColumn = tiTableInfo.getColumn(colName)
        val colOffset = tiColumn.getOffset
        val dataType = tiColumn.getType

        val ordering: Ordering[WrappedEncodedRow] = new Ordering[WrappedEncodedRow] {
          override def compare(x: WrappedEncodedRow, y: WrappedEncodedRow): Int = {
            val xIndex = x.row.get(colOffset, dataType)
            val yIndex = y.row.get(colOffset, dataType)
            xIndex match {
              case _: java.lang.Integer =>
                xIndex
                  .asInstanceOf[java.lang.Integer]
                  .compareTo(yIndex.asInstanceOf[java.lang.Integer])
              case _: java.lang.Long =>
                xIndex
                  .asInstanceOf[java.lang.Long]
                  .compareTo(yIndex.asInstanceOf[java.lang.Long])
              case _: java.lang.Double =>
                xIndex
                  .asInstanceOf[java.lang.Double]
                  .compareTo(yIndex.asInstanceOf[java.lang.Double])
              case _: java.lang.Float =>
                xIndex
                  .asInstanceOf[java.lang.Float]
                  .compareTo(yIndex.asInstanceOf[java.lang.Float])
              case _: Array[Byte] =>
                FastByteComparisons.compareTo(
                  xIndex.asInstanceOf[Array[Byte]],
                  yIndex.asInstanceOf[Array[Byte]])
              case _ => xIndex.toString.compareTo(yIndex.toString)
            }
          }
        }

        val rdd = wrappedEncodedRdd(index.getId)

        val regionSplitNum = if (options.regionSplitNum != 0) {
          options.regionSplitNum
        } else {
          val indexSize = calcSize(rdd)
          logger.info(s"count=$count indexSize=$indexSize")
          val splitNum = estimateRegionSplitNum(indexSize)
          logger.info(s"index region split num=$splitNum")
          splitNum
        }

        // region split
        if (regionSplitNum > 1) {
          logger.info(
            s"index region split, regionSplitNum=$regionSplitNum, indexName=${index.getName}")
          if (count > (regionSplitNum * 1000 + 1) * 10) {
            logger.info("split by sample data")
            val frac = options.sampleSplitFrac
            val sampleData = rdd.takeSample(false, (regionSplitNum * frac + 1).toInt)
            val sortedSampleData = sampleData.sorted(ordering)
            val buf = new StringBuilder
            for (i <- 1 until regionSplitNum.toInt) {
              val indexValue = toString(sortedSampleData(i * frac).row.get(colOffset, dataType))
              buf.append(" (")
              buf.append("\"")
              buf.append(indexValue)
              buf.append("\"")
              buf.append(")")
              if (i != regionSplitNum - 1) {
                buf.append(",")
              }
            }
            try {
              tiDBJDBCClient
                .splitIndexRegion(options.database, options.table, index.getName, buf.toString())
            } catch {
              case e: SQLException => throw e
            }
          } else {
            logger.info("split by min/max data")
            val (minIndexValue, maxIndexValue) = calcIndexMinMax(rdd, index)
            logger.info(s"index min=$minIndexValue max=$maxIndexValue")
            try {
              tiDBJDBCClient
                .splitIndexRegion(
                  options.database,
                  options.table,
                  index.getName,
                  minIndexValue.toString,
                  maxIndexValue.toString,
                  regionSplitNum)
            } catch {
              case e: SQLException =>
                if (options.isTest) {
                  throw e
                }
            }
          }
        } else {
          logger.info(
            s"skip index split index, regionSplitNum=$regionSplitNum, indexName=${index.getName}")
        }
      }
    }
  }

  // when data to be inserted is too small to do region split, we check is user set region split num.
  // If so, we do region split as user's intention. This is also useful for writing test case.
  // We assume the data to be inserted is ruled by normal distribution.
  private def splitTableRegion(wrappedRowRdd: RDD[WrappedEncodedRow]): Unit = {
    if (options.enableRegionSplit && isEnableSplitRegion) {
      if (options.regionSplitNum != 0) {
        try {
          tiDBJDBCClient
            .splitTableRegion(
              options.database,
              options.table,
              0,
              Int.MaxValue,
              options.regionSplitNum)
        } catch {
          case e: SQLException =>
            if (options.isTest) {
              throw e
            }
        }
      } else {

        val regionSplitNum = if (options.regionSplitNum != 0) {
          options.regionSplitNum
        } else {
          val recordSize = calcSize(wrappedRowRdd)
          val splitNum = estimateRegionSplitNum(recordSize)
          logger.info(s"record region split num=$splitNum")
          splitNum
        }
        // region split
        if (regionSplitNum > 1) {
          val (minHandle, maxHandle) = calcRecordMinMax(wrappedRowRdd)
          logger.info(s"record min=$minHandle max=$maxHandle")
          if (checkTidbRegionSplitContidion(minHandle, maxHandle, regionSplitNum)) {
            logger.info(s"table region split is enabled, regionSplitNum=$regionSplitNum")
            try {
              tiDBJDBCClient
                .splitTableRegion(
                  options.database,
                  options.table,
                  minHandle,
                  maxHandle,
                  regionSplitNum)
            } catch {
              case e: SQLException =>
                if (options.isTest) {
                  throw e
                }
            }
          } else {
            logger.info("table region split is skipped")
          }
        }
      }
    }
  }
}
