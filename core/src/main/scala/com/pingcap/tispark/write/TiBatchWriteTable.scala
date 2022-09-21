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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.write

import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.allocator.RowIDAllocator.RowIDAllocatorType
import com.pingcap.tikv.codec.TableCodec
import com.pingcap.tikv.handle.{Handle, IntHandle}
import com.pingcap.tikv.key.{IndexKey, RowKey}
import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType
import com.pingcap.tikv.meta.{TiColumnInfo, TiDBInfo, TiIndexInfo}
import com.pingcap.tikv.partition.TableCommon
import com.pingcap.tikv.{ClientSession, TiConfiguration, TiDBJDBCClient}
import com.pingcap.tispark.TiTableReference
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.WriteUtil.locatePhysicalTable
import com.pingcap.tispark.utils.{SchemaUpdateTime, TiUtil, WriteUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.slf4j.LoggerFactory
import org.tikv.common.BytePairWrapper
import org.tikv.common.exception.TiBatchWriteException
import org.tikv.common.meta.TiTimestamp

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

class TiBatchWriteTable(
    @transient var df: DataFrame,
    @transient val tiContext: TiContext,
    val options: TiDBOptions,
    val tiConf: TiConfiguration,
    val isTiDBV4: Boolean,
    val tiTable: TableCommon)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._

  @transient private val clientSession = tiContext.clientSession
  // only fetch row format version once for each batch write process

  private val enableNewRowFormat: Boolean = options.tidbRowFormatVersion == 2
  private val tiTableRef: TiTableReference = options.getTiTableRef(tiConf)
  private val tiDBInfo: TiDBInfo = clientSession.getCatalog.getDatabase(tiTableRef.databaseName)
  private val tiTableInfo = tiTable.getTableInfo
  private val tableColSize: Int = tiTableInfo.getColumns.size()
  private val colsMapInTiDB: Map[String, TiColumnInfo] =
    tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
  private var colsInDf: List[String] = df.columns.toList.map(_.toLowerCase())
  private val uniqueIndices: Seq[TiIndexInfo] =
    tiTableInfo.getIndices.asScala.filter(index => index.isUnique)
  private val handleCol: TiColumnInfo = tiTableInfo.getPKIsHandleColumn
  private var tableLocked: Boolean = false
  private var autoIncProvidedID: Boolean = false
  private var autoRandomProvidedID: Boolean = false
  // isCommonHandle = true => clustered index
  private val isCommonHandle = tiTableInfo.isCommonHandle
  private var deltaCount: Long = 0
  private var modifyCount: Long = 0
  @transient private var persistedDFList: List[DataFrame] = Nil
  @transient private var persistedRDDList: List[RDD[_]] = Nil

  def persist(): Unit = {
    df = df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedDFList = df :: persistedDFList
  }

  def unpersistAll(): Unit = {
    persistedDFList.foreach(_.unpersist())
    persistedRDDList.foreach(_.unpersist())
  }

  def isDFEmpty: Boolean = {
    if (TiUtil.isDataFrameEmpty(df.select(df.columns.head))) {
      logger.warn(s"the dataframe write to $tiTableRef is empty!")
      true
    } else {
      false
    }
  }

  def buildSchemaUpdateTime(): SchemaUpdateTime = {
    SchemaUpdateTime(
      tiTableRef.databaseName,
      tiTableRef.tableName,
      tiTableInfo.getUpdateTimestamp)
  }

  def preCalculate(startTimeStamp: TiTimestamp): RDD[(SerializableKey, Array[Byte])] = {
    val sc = tiContext.sparkSession.sparkContext

    val count = df.count
    logger.info(s"source data count=$count")

    // a rough estimate to deltaCount and modifyCount
    deltaCount = count
    modifyCount = count

    val rdd = if (tiTableInfo.hasAutoIncrementColumn && isNeedAllocateIdForAutoIncrementCol(df)) {
      // auto increment
      allocateIdForAutoIDCol(count, startTimeStamp, RowIDAllocatorType.AUTO_INCREMENT)
    } else if (tiTableInfo.hasAutoRandomColumn && isNeedAllocateIdForAutoRandomCol(df)) {
      // auto random
      allocateIdForAutoIDCol(count, startTimeStamp, RowIDAllocatorType.AUTO_RANDOM)
    } else {
      df.rdd
    }

    // spark row -> tikv row
    val tiRowRdd = rdd.map(row => WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf))

    // check value not null
    checkValueNotNull(tiRowRdd)

    val wrappedRowRdd: RDD[WrappedRow] = if (isCommonHandle || tiTableInfo.isPkHandle) {
      tiRowRdd.map { row =>
        WrappedRow(row, WriteUtil.extractHandle(row, tiTableInfo))
      }
    } else {
      // For rows with a null primary key, we need to assign RowIDs as handle to these inserted rows.
      val rowIDAllocator =
        getRowIDAllocator(count, startTimeStamp, RowIDAllocatorType.IMPLICIT_ROWID)
      tiRowRdd.zipWithIndex.map { row =>
        val index = row._2 + 1
        val rowId = rowIDAllocator.getShardRowId(index)
        WrappedRow(row._1, new IntHandle(rowId))
      }
    }

    // for partition table, we need calculate each row and tell which physical table
    // that row is belong to.
    // currently we only support replace and insert.
    val constraintCheckAndDeduplicateIsNeeded =
      isCommonHandle || handleCol != null || uniqueIndices.nonEmpty

    val keyValueRDD = if (constraintCheckAndDeduplicateIsNeeded) {
      // since the primary key or unique index in the inserted column may be duplicated,
      // duplicate data needs to be removed here.
      val distinctWrappedRowRdd = deduplicate(wrappedRowRdd)
      if (!options.deduplicate && wrappedRowRdd.count() != distinctWrappedRowRdd
          .count()) {
        throw new TiBatchWriteException("duplicate unique key or primary key")
      }
      val insertRowRdd = generateRecordKV(distinctWrappedRowRdd, remove = false)
      val insertIndexRdd =
        WriteUtil.generateIndexKVRDD(sc, distinctWrappedRowRdd, tiTable, remove = false)

      // The rows that exist in the current TiDB that conflict
      // with the primary key or unique index of the inserted rows.
      val conflictRows = (if (options.useSnapshotBatchGet) {
                            generateDataToBeRemovedRddV2(distinctWrappedRowRdd, startTimeStamp)
                          } else {
                            generateDataToBeRemovedRddV1(distinctWrappedRowRdd, startTimeStamp)
                          }).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      persistedRDDList = conflictRows :: persistedRDDList

      if (!options.replace && !conflictRows.isEmpty()) {
        throw new TiBatchWriteException("data to be inserted has conflicts with TiKV data")
      }
      if ((autoIncProvidedID || autoRandomProvidedID) && conflictRows.count() != count) {
        throw new TiBatchWriteException(
          "currently user provided auto id value is only supported in update mode!")
      }

      val deleteRowRDD = generateRecordKV(conflictRows, remove = true)
      val deleteIndexRDD = WriteUtil.generateIndexKVRDD(sc, conflictRows, tiTable, remove = true)

      (unionInsertDelete(insertRowRdd, deleteRowRDD) ++
        unionInsertDelete(insertIndexRdd, deleteIndexRDD)).map(obj =>
        (obj.encodedKey, obj.encodedValue))

    } else {
      val insertRowRdd = generateRecordKV(wrappedRowRdd, remove = false)
      val insertIndexRdd =
        WriteUtil.generateIndexKVRDD(sc, wrappedRowRdd, tiTable, remove = false)
      (insertRowRdd ++ insertIndexRdd).map(obj => (obj.encodedKey, obj.encodedValue))
    }

    // persist
    val persistedKeyValueRDD =
      keyValueRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedRDDList = persistedKeyValueRDD :: persistedRDDList
    persistedKeyValueRDD
  }

  private def isNeedAllocateIdForAutoIncrementCol(df: DataFrame): Boolean = {
    val provideIDRowNum = getProvideIDRowNum(df, tiTableInfo.getAutoIncrementColInfo.getName)
    if (provideIDRowNum == 0) {
      return true
    }
    if (provideIDRowNum == df.count()) {
      if (!options.replace) {
        val colNames =
          tiTableInfo.getColumns.asScala.map(col => col.getName).mkString(", ")
        throw new TiBatchWriteException(
          s"""currently user provided auto increment value is only supported in update mode!
             |please set parameter replace to true!
             |
             |colsInDf.length = ${colsInDf.length}
             |
             |df.schema = ${df.schema}
             |
             |tableColSize = $tableColSize
             |
             |colNames = $colNames
             |
             |tiTableInfo = $tiTableInfo
            """.stripMargin)
      }
      autoIncProvidedID = true
      return false;
    }
    throw new TiBatchWriteException(
      "cannot allocate id on the condition of having null value and valid value on auto increment column")
  }

  private def isNeedAllocateIdForAutoRandomCol(df: DataFrame): Boolean = {
    val provideIDRowNum = getProvideIDRowNum(df, tiTableInfo.getAutoRandomColInfo.getName)
    if (provideIDRowNum == 0) {
      return true
    }
    if (provideIDRowNum == df.count()) {
      if (!options.replace) {
        val colNames =
          tiTableInfo.getColumns.asScala.map(col => col.getName).mkString(", ")
        throw new TiBatchWriteException(
          s"""currently user provided auto random value is only supported in update mode!
             |please set parameter replace to true!
             |
             |colsInDf.length = ${colsInDf.length}
             |
             |df.schema = ${df.schema}
             |
             |tableColSize = $tableColSize
             |
             |colNames = $colNames
             |
             |tiTableInfo = $tiTableInfo
            """.stripMargin)
      }
      autoRandomProvidedID = true
      return false;
    }
    throw new TiBatchWriteException(
      "cannot allocate id on the condition of having null value and valid value on auto random column")
  }

  private def getProvideIDRowNum(df: DataFrame, colName: String): Long = {
    if (!colsInDf.contains(colName)) {
      return 0
    }
    df.select(colName)
      .rdd
      .filter { row => row.get(0) != null }
      .count()
  }

  private def allocateIdForAutoIDCol(
      count: Long,
      timestamp: TiTimestamp,
      allocatorType: RowIDAllocatorType): RDD[Row] = {

    val rowIDAllocator = getRowIDAllocator(count, timestamp, allocatorType)
    val (isAutoIDCol, allocatorID, autoIDColName) = allocatorType match {
      case RowIDAllocatorType.AUTO_INCREMENT =>
        (
          (columnName: String) => {
            colsMapInTiDB(columnName).isAutoIncrement
          },
          (index: Long) => {
            rowIDAllocator.getAutoIncId(index)
          },
          tiTableInfo.getAutoIncrementColInfo.getName)
      case RowIDAllocatorType.AUTO_RANDOM =>
        (
          (columnName: String) => {
            colsMapInTiDB(columnName).isPrimaryKey
          },
          (index: Long) => {
            rowIDAllocator.getAutoRandomId(index)
          },
          tiTableInfo.getAutoRandomColInfo.getName)
    }
    // if auto increment column is not provided, we need allocate id for it.
    if (colsInDf.contains(autoIDColName)) {
      df.drop(autoIDColName)
    }
    val newDf = df.withColumn(autoIDColName, lit(null).cast("long"))
    // update colsInDF since we just add one column in df
    colsInDf = newDf.columns.toList.map(_.toLowerCase())
    // last one is auto increment column
    newDf.rdd.zipWithIndex.map { row =>
      val rowSep = row._1.toSeq.zipWithIndex.map { data =>
        val colOffset = data._2
        if (colsMapInTiDB.contains(colsInDf(colOffset))) {
          if (isAutoIDCol(colsInDf(colOffset))) {
            val index = row._2 + 1
            allocatorID(index)
          } else {
            data._1
          }
        }
      }
      Row.fromSeq(rowSep)
    }
  }

  // if rdd contains same key, it means we need first delete the old value and insert the new value associated the
  // key. We can merge the two operation into one update operation.
  private def unionInsertDelete(
      insert: RDD[WrappedEncodedRow],
      delete: RDD[WrappedEncodedRow]): RDD[WrappedEncodedRow] = {
    (insert ++ delete)
      .map(wrappedEncodedRow => (wrappedEncodedRow.encodedKey, wrappedEncodedRow))
      .reduceByKey { (r1, r2) =>
        // Note: the deletion operation's value of kv pair is empty.
        if (r1.encodedValue.isEmpty) r2 else r1
      }
      .map(_._2)
  }

  def checkUnsupported(): Unit = {

    // Only RangePartition and HashPartition are supported
    if (tiTableInfo.isPartitionEnabled) {
      val pType = tiTableInfo.getPartitionInfo.getType
      if (pType != PartitionType.RangePartition && pType != PartitionType.HashPartition) {
        throw new UnsupportedOperationException(s"Unsupported partition type: $pType")
      }
    }

    // write to table with generated column
    if (tiTableInfo.hasGeneratedColumn) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to table with generated column!")
    }
  }

  def checkAuthorization(tiAuthorization: Option[TiAuthorization], options: TiDBOptions): Unit = {
    if (options.replace) {
      TiAuthorization.authorizeForInsert(tiTableInfo.getName, tiDBInfo.getName, tiAuthorization)
      TiAuthorization.authorizeForDelete(tiTableInfo.getName, tiDBInfo.getName, tiAuthorization)
    } else {
      TiAuthorization.authorizeForInsert(tiTableInfo.getName, tiDBInfo.getName, tiAuthorization)
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

  // update table statistics: modify_count & count
  def updateTableStatistics(startTs: Long, tiDBJDBCClient: TiDBJDBCClient): Unit = {
    try {
      tiDBJDBCClient.updateTableStatistics(startTs, tiTableInfo.getId, deltaCount, modifyCount)
    } catch {
      case e: Throwable => logger.warn("updateTableStatistics error!", e)
    }
  }

  private def getRowIDAllocator(
      step: Long,
      timestamp: TiTimestamp,
      allocatorType: RowIDAllocatorType): RowIDAllocator = {
    RowIDAllocator.createRowIDAllocator(
      tiDBInfo.getId,
      tiTableInfo,
      tiConf,
      step,
      timestamp,
      allocatorType)
  }

  private def isNullUniqueIndexValue(value: Array[Byte]): Boolean = {
    value.length == 1 && value(0) == '0'
  }

  private def generateDataToBeRemovedRddV1(
      rdd: RDD[WrappedRow],
      startTs: TiTimestamp): RDD[WrappedRow] = {
    rdd
      .mapPartitions { wrappedRows =>
        val snapshot =
          ClientSession.getInstance(tiConf).createSnapshot(startTs.getPrevious)
        wrappedRows.map { wrappedRow =>
          val rowBuf = mutable.ListBuffer.empty[WrappedRow]
          //  check handle key
          if (handleCol != null || isCommonHandle) {
            val oldValue = snapshot.get(buildRowKey(wrappedRow.row, wrappedRow.handle).bytes)
            if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
              val oldRow = TableCodec.decodeRow(oldValue, wrappedRow.handle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, wrappedRow.handle)
            }
          }

          uniqueIndices.foreach { index =>
            if (!isCommonHandle || !index.isPrimary) {
              val keyInfo = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)
              // if handle is appended, it must not exists in old table
              if (!keyInfo._2) {
                val oldValue = snapshot.get(keyInfo._1.bytes)
                if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
                  val oldHandle =
                    TableCodec.decodeHandleInUniqueIndexValue(oldValue, isCommonHandle)
                  val oldRowValue = snapshot.get(buildRowKey(wrappedRow.row, oldHandle).bytes)
                  val oldRow = TableCodec.decodeRow(oldRowValue, oldHandle, tiTableInfo)
                  rowBuf += WrappedRow(oldRow, oldHandle)
                }
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
      val snapshot =
        ClientSession.getInstance(tiConf).createSnapshot(startTs.getPrevious)
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

        def genNextUniqueIndexBatch(
            batch: List[WrappedRow],
            index: TiIndexInfo): util.List[Array[Byte]] = {
          val keyList = new util.ArrayList[Array[Byte]]()
          batch.foreach { wrappedRow =>
            val encodeResult = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)
            if (encodeResult._2) {
              // only add the key if the key is distinct.
              keyList.add(encodeResult._1.bytes)
            }
          }
          keyList
        }

        def processHandleDelete(oldValueList: java.util.List[BytePairWrapper]): Unit = {
          for (i <- 0 until oldValueList.size) {
            val oldValuePair = oldValueList.get(i)
            val oldValue = oldValuePair.getValue
            val oldKey = oldValuePair.getKey

            if (oldValue.nonEmpty && !isNullUniqueIndexValue(oldValue)) {
              val oldHandle = RowKey.decode(oldKey).getHandle
              val oldRow = TableCodec.decodeRow(oldValue, oldHandle, tiTableInfo)
              rowBuf += WrappedRow(oldRow, oldHandle)
            }
          }
        }

        def buildRowKeyFromConflictIndexRow(
            conflictIndexRow: java.util.List[BytePairWrapper]): mutable.Set[Array[Byte]] = {
          val conflictRowKey = mutable.Set.empty[Array[Byte]]
          for (i <- 0 until conflictIndexRow.size) {
            val conflictUniqueIndexValue = conflictIndexRow.get(i).getValue
            if (conflictUniqueIndexValue.nonEmpty && !isNullUniqueIndexValue(
                conflictUniqueIndexValue)) {
              val conflictHandle =
                TableCodec.decodeHandleInUniqueIndexValue(
                  conflictUniqueIndexValue,
                  isCommonHandle)
              val conflictUniqueIndexRowKey =
                RowKey.toRowKey(locatePhysicalTable(tiTable), conflictHandle);
              conflictRowKey.add(conflictUniqueIndexRowKey.getBytes)
            }
          }
          conflictRowKey
        }

        def processNextBatch(): Unit = {
          rowBuf = mutable.ListBuffer.empty[WrappedRow]
          val mayConflictRowKeys = mutable.Set.empty[Array[Byte]]
          val batch = getNextBatch(wrappedRows)

          // get the handle of the row which the unique index conflict.
          uniqueIndices.foreach { index =>
            if (!isCommonHandle || !index.isPrimary) {
              val batchIndices = genNextUniqueIndexBatch(batch, index)
              // get all conflict unique index`s value
              val conflictUniqueIndexRows =
                snapshot.batchGet(options.batchGetBackOfferMS, batchIndices)
              mayConflictRowKeys ++= buildRowKeyFromConflictIndexRow(conflictUniqueIndexRows)
            }
          }

          if (handleCol != null || isCommonHandle) {
            // add all cluster key that insert to tikv.
            batch.foreach { wrappedRow =>
              mayConflictRowKeys.add(buildRowKey(wrappedRow.row, wrappedRow.handle).bytes)
            }
          }
          val conflictRow =
            snapshot.batchGet(options.batchGetBackOfferMS, mayConflictRowKeys.toList.asJava)
          // extract handle of conflictRow
          processHandleDelete(conflictRow)

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

  private def buildRowKey(row: TiRow, handle: Handle): SerializableKey = {
    new SerializableKey(RowKey.toRowKey(WriteUtil.locatePhysicalTable(tiTable), handle).getBytes)
  }

  private def buildUniqueIndexKey(
      row: TiRow,
      handle: Handle,
      index: TiIndexInfo): (SerializableKey, Boolean) = {
    // NULL is only allowed in unique key, primary key does not allow NULL value
    val encodeResult =
      IndexKey.genIndexKey(locatePhysicalTable(tiTable), row, index, handle, tiTable.getTableInfo)
    (new SerializableKey(encodeResult.indexKey), encodeResult.distinct)
  }

  private def generateRowKey(
      row: TiRow,
      handle: Handle,
      remove: Boolean): (SerializableKey, Array[Byte]) = {
    if (remove) {
      (buildRowKey(row, handle), new Array[Byte](0))
    } else {
      (
        new SerializableKey(
          RowKey.toRowKey(WriteUtil.locatePhysicalTable(tiTable), handle).getBytes),
        encodeTiRow(row))
    }
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
}
