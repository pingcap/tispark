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
import com.pingcap.tikv.codec.TableCodec
import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.key.{Handle, IndexKey, IntHandle, RowKey}
import com.pingcap.tikv.meta._
import com.pingcap.tikv.{BytePairWrapper, TiConfiguration, TiDBJDBCClient, TiSession}
import com.pingcap.tispark.TiTableReference
import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.utils.{SchemaUpdateTime, TiUtil, WriteUtil}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{TiContext, _}
import org.slf4j.LoggerFactory

import java.util
import scala.collection.JavaConverters._
import scala.collection.mutable

class TiBatchWriteTable(
    @transient var df: DataFrame,
    @transient val tiContext: TiContext,
    val options: TiDBOptions,
    val tiConf: TiConfiguration,
    @transient val tiDBJDBCClient: TiDBJDBCClient,
    val isTiDBV4: Boolean)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._
  @transient private val tiSession = tiContext.tiSession
  // only fetch row format version once for each batch write process
  private val enableNewRowFormat: Boolean =
    if (isTiDBV4) tiDBJDBCClient.getRowFormatVersion == 2 else false
  private var tiTableRef: TiTableReference = _
  private var tiDBInfo: TiDBInfo = _
  private var tiTableInfo: TiTableInfo = _
  private var tableColSize: Int = _
  private var colsMapInTiDB: Map[String, TiColumnInfo] = _
  private var colsInDf: List[String] = _
  private var uniqueIndices: Seq[TiIndexInfo] = _
  private var handleCol: TiColumnInfo = _
  private var tableLocked: Boolean = false
  private var autoIncProvidedID: Boolean = false
  // isCommonHandle = true => clustered index
  private var isCommonHandle: Boolean = _
  private var deltaCount: Long = 0
  private var modifyCount: Long = 0
  @transient private var persistedDFList: List[DataFrame] = Nil
  @transient private var persistedRDDList: List[RDD[_]] = Nil

  tiTableRef = options.getTiTableRef(tiConf)
  tiDBInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
  tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)

  if (tiTableInfo == null) {
    throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
  }

  isCommonHandle = tiTableInfo.isCommonHandle
  colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
  colsInDf = df.columns.toList.map(_.toLowerCase())
  uniqueIndices = tiTableInfo.getIndices.asScala.filter(index => index.isUnique)
  handleCol = tiTableInfo.getPKIsHandleColumn
  tableColSize = tiTableInfo.getColumns.size()

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

    // auto increment
    val rdd = if (tiTableInfo.hasAutoIncrementColumn) {
      val autoIncrementColName = tiTableInfo.getAutoIncrementColInfo.getName

      def allNullOnAutoIncrement: Boolean = {
        df.select(autoIncrementColName)
          .rdd
          .filter { row => row.get(0) != null }
          .isEmpty()
      }

      def isProvidedID: Boolean = {
        if (tableColSize != colsInDf.length) {
          false
        } else {
          if (allNullOnAutoIncrement) {
            df = df.drop(autoIncrementColName)
            false
          } else {
            true
          }
        }
      }

      // when auto increment column is provided but the corresponding column in df contains null,
      // we need throw exception
      if (isProvidedID) {
        autoIncProvidedID = true

        if (!options.replace) {

          val colNames = tiTableInfo.getColumns.asScala.map(col => col.getName).mkString(", ")
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

        if (!colsInDf.contains(autoIncrementColName)) {
          throw new TiBatchWriteException(
            "Column size is matched but cannot find auto increment column by name")
        }

        val hasNullValue = !df
          .select(autoIncrementColName)
          .rdd
          .filter { row => row.get(0) == null }
          .isEmpty()
        if (hasNullValue) {
          throw new TiBatchWriteException(
            "cannot allocate id on the condition of having null value and valid value on auto increment column")
        }
        df.rdd
      } else {
        // if auto increment column is not provided, we need allocate id for it.
        // adding an auto increment column to df
        val newDf = df.withColumn(autoIncrementColName, lit(null).cast("long"))
        val rowIDAllocator = getRowIDAllocator(count)

        // update colsInDF since we just add one column in df
        colsInDf = newDf.columns.toList.map(_.toLowerCase())
        // last one is auto increment column
        newDf.rdd.zipWithIndex.map { row =>
          val rowSep = row._1.toSeq.zipWithIndex.map { data =>
            val colOffset = data._2
            if (colsMapInTiDB.contains(colsInDf(colOffset))) {
              if (colsMapInTiDB(colsInDf(colOffset)).isAutoIncrement) {
                val index = row._2 + 1
                rowIDAllocator.getAutoIncId(index)
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
    val tiRowRdd = rdd.map(row => WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf))

    // check value not null
    checkValueNotNull(tiRowRdd)

    val wrappedRowRdd: RDD[WrappedRow] = if (isCommonHandle || tiTableInfo.isPkHandle) {
      val noDistinctWrappedRowRdd = tiRowRdd.map { row =>
        WrappedRow(row, WriteUtil.extractHandle(row, tiTableInfo))
      }
      val distinctWrappedRowRdd = deduplicate(noDistinctWrappedRowRdd)
      if (!options.deduplicate && noDistinctWrappedRowRdd.count() != distinctWrappedRowRdd
          .count()) {
        throw new TiBatchWriteException("duplicate unique key or primary key")
      }
      distinctWrappedRowRdd
    } else {
      val rowIDAllocator = getRowIDAllocator(count)
      tiRowRdd.zipWithIndex.map { row =>
        val index = row._2 + 1
        val rowId = rowIDAllocator.getShardRowId(index)
        WrappedRow(row._1, new IntHandle(rowId))
      }
    }
    val insertRowRdd = generateRecordKV(wrappedRowRdd, remove = false)
    val insertIndexRdd = WriteUtil.generateIndexKV(sc, wrappedRowRdd, tiTableInfo, remove = false)

    // for partition table, we need calculate each row and tell which physical table
    // that row is belong to.
    // currently we only support replace and insert.
    val constraintCheckIsNeeded = isCommonHandle || handleCol != null || uniqueIndices.nonEmpty

    val keyValueRDD = if (constraintCheckIsNeeded) {

      val conflictRows = (if (options.useSnapshotBatchGet) {
                            generateDataToBeRemovedRddV2(wrappedRowRdd, startTimeStamp)
                          } else {
                            generateDataToBeRemovedRddV1(wrappedRowRdd, startTimeStamp)
                          }).persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
      persistedRDDList = conflictRows :: persistedRDDList

      if (!options.replace && !conflictRows.isEmpty()) {
        throw new TiBatchWriteException("data to be inserted has conflicts with TiKV data")
      }

      if (autoIncProvidedID && conflictRows.count() != count) {
        throw new TiBatchWriteException(
          "currently user provided auto increment value is only supported in update mode!")
      }
      val deleteRowRDD = generateRecordKV(conflictRows, remove = true)
      val deleteIndexRDD = WriteUtil.generateIndexKV(sc, conflictRows, tiTableInfo, remove = true)

      def unionInsertDelete(
          insert: RDD[WrappedEncodedRow],
          delete: RDD[WrappedEncodedRow]): RDD[WrappedEncodedRow] = {
        (insert ++ delete)
          .map(wrappedEncodedRow => (wrappedEncodedRow.encodedKey, wrappedEncodedRow))
          .reduceByKey { (r1, r2) =>
            // if rdd contains same key, it means we need first delete the old value and insert the new value associated the
            // key. We can merge the two operation into one update operation.
            // Note: the deletion operation's value of kv pair is empty.
            if (r1.encodedValue.isEmpty) r2 else r1
          }
          .map(_._2)
      }

      (unionInsertDelete(insertRowRdd, deleteRowRDD) ++
        unionInsertDelete(insertIndexRdd, deleteIndexRDD)).map(obj =>
        (obj.encodedKey, obj.encodedValue))
    } else {

      (insertRowRdd ++ insertIndexRdd).map(obj => (obj.encodedKey, obj.encodedValue))
    }

    // persist
    val persistedKeyValueRDD =
      keyValueRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedRDDList = persistedKeyValueRDD :: persistedRDDList
    persistedKeyValueRDD
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
    // write to table with auto random column
    if (tiTableInfo.hasAutoRandomColumn) {
      throw new TiBatchWriteException(
        "tispark currently does not support write data to table with auto random column!")
    }

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
  def updateTableStatistics(startTs: Long): Unit = {
    try {
      tiDBJDBCClient.updateTableStatistics(startTs, tiTableInfo.getId, deltaCount, modifyCount)
    } catch {
      case e: Throwable => logger.warn("updateTableStatistics error!", e)
    }
  }

  private def getRowIDAllocator(step: Long): RowIDAllocator = {
    RowIDAllocator.create(
      tiDBInfo.getId,
      tiTableInfo,
      tiConf,
      tiTableInfo.isAutoIncColUnsigned,
      step)
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
                    TableCodec.decodeIndexValueForClusteredIndexVersion1(oldValue, isCommonHandle)
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

        def genNextUniqueIndexBatch(
            batch: List[WrappedRow],
            index: TiIndexInfo): util.List[Array[Byte]] = {
          val keyList = new util.ArrayList[Array[Byte]]()
          batch.foreach { wrappedRow =>
            val encodeResult = buildUniqueIndexKey(wrappedRow.row, wrappedRow.handle, index)
            if (!encodeResult._2) {
              // only add the key if handle is not appended, since if handle is appened,
              // the value must be a new value
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

        def processNextBatch(): Unit = {
          rowBuf = mutable.ListBuffer.empty[WrappedRow]
          val mayConflictRowKeys = mutable.Set.empty[Array[Byte]]
          val batch = getNextBatch(wrappedRows)

          // get the handle of the row which the unique index conflict.
          uniqueIndices.foreach { index =>
            if (!isCommonHandle || !index.isPrimary) {
              val batchIndices = genNextUniqueIndexBatch(batch, index)
              val conflictUniqueIndexRows =
                snapshot.batchGet(options.batchGetBackOfferMS, batchIndices)
              for (i <- 0 until conflictUniqueIndexRows.size) {
                val conflictUniqueIndexValue = conflictUniqueIndexRows.get(i).getValue
                if (conflictUniqueIndexValue.nonEmpty && !isNullUniqueIndexValue(
                    conflictUniqueIndexValue)) {
                  val conflictHandle = TableCodec.decodeIndexValueForClusteredIndexVersion1(
                    conflictUniqueIndexValue,
                    isCommonHandle)
                  // TODO
                  // change to use physical id
                  val conflictUniqueIndexRowKey =
                    RowKey.toRowKey(tiTableInfo.getId, conflictHandle);
                  mayConflictRowKeys.add(conflictUniqueIndexRowKey.getBytes)
                }
              }
            }
          }
          if (handleCol != null || isCommonHandle) {
            batch.foreach { wrappedRow =>
              mayConflictRowKeys.add(buildRowKey(wrappedRow.row, wrappedRow.handle).bytes)
            }
          }
          val conflictRow =
            snapshot.batchGet(options.batchGetBackOfferMS, mayConflictRowKeys.toList.asJava)
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
    new SerializableKey(
      RowKey.toRowKey(WriteUtil.locatePhysicalTable(row, tiTableInfo), handle).getBytes)
  }

  private def buildUniqueIndexKey(
      row: TiRow,
      handle: Handle,
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
      IndexKey.toIndexKey(WriteUtil.locatePhysicalTable(row, tiTableInfo), index.getId, keys: _*)
    (new SerializableKey(indexKey.getBytes), encodeResult.appendHandle)
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
          RowKey.toRowKey(WriteUtil.locatePhysicalTable(row, tiTableInfo), handle).getBytes),
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
