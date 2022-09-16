/*
 * Copyright 2021 PingCAP, Inc.
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

package com.pingcap.tispark.utils

import com.pingcap.tikv.codec.TableCodec
import com.pingcap.tikv.handle.{CommonHandle, Handle, IntHandle}
import com.pingcap.tikv.key.{IndexKey, RowKey}
import com.pingcap.tikv.meta.{TiIndexColumn, TiIndexInfo, TiTableInfo}
import com.pingcap.tikv.partition.TableCommon
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.types.DataType
import com.pingcap.tispark.write.TiBatchWrite.{SparkRow, TiRow}
import com.pingcap.tispark.write.{SerializableKey, WrappedEncodedRow, WrappedRow}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.tikv.common.exception.{
  ConvertOverflowException,
  TiBatchWriteException,
  TiDBConvertException
}

import scala.collection.JavaConverters._
import scala.collection.mutable

object WriteUtil {

  /**
   * Convert spark's row to tikv row. We do not allocate handle for no pk case.
   * allocating handle id will be finished after we check conflict.
   *
   * @param sparkRow
   * @param tiTableInfo
   * @param df
   * @return
   */
  def sparkRow2TiKVRow(
      sparkRow: SparkRow,
      tiTableInfo: TiTableInfo,
      colsInDf: List[String]): TiRow = {
    val colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap

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

  /**
   * ExtractHandle from isCommonHandle or isPkHandle
   * For isPkHandle: build IntHandle with pk
   * For isCommonHandle: build CommonHandle with pk
   *
   * @param row
   * @param tiTableInfo
   * @return
   */
  def extractHandle(row: TiRow, tiTableInfo: TiTableInfo): Handle = {
    // If handle ID is changed when update, update will remove the old record first, and then call `AddRecord` to add a new record.
    // Currently, only insert can set _tidb_rowid, update can not update _tidb_rowid.
    val handleCol = tiTableInfo.getPKIsHandleColumn
    if (tiTableInfo.isCommonHandle) {
      var dataTypeList: List[DataType] = Nil
      var dataList: List[Object] = Nil
      var indexColumnList: List[TiIndexColumn] = Nil
      tiTableInfo.getPrimaryKey.getIndexColumns.forEach { idx =>
        val col = tiTableInfo.getColumn(idx.getName)
        dataTypeList = col.getType :: dataTypeList
        dataList = row.get(col.getOffset, col.getType) :: dataList
        indexColumnList = idx :: indexColumnList
      }
      dataTypeList = dataTypeList.reverse
      dataList = dataList.reverse
      indexColumnList = indexColumnList.reverse
      CommonHandle.newCommonHandle(
        dataTypeList.toArray,
        dataList.toArray,
        indexColumnList.map(_.getLength).toArray)
    } else if (tiTableInfo.isPkHandle) {
      val id = row
        .get(handleCol.getOffset, handleCol.getType)
        .asInstanceOf[java.lang.Long]
      new IntHandle(id)
    } else {
      throw new TiBatchWriteException(
        "Cannot extract handle from non-isCommonHandle and non-isPkHandle")
    }
  }

  /**
   * Generate Record that will be removed
   * key: tableId + handle
   * value: empty
   *
   * @param rdd
   * @param tableId
   * @return
   */
  def generateRecordKVRDDToDelete(rdd: RDD[WrappedRow], tableId: Long): RDD[WrappedEncodedRow] = {
    rdd.map { wrappedRow =>
      {
        generateRecordKVToDelete(wrappedRow, tableId)
      }
    }
  }

  def generateRecordKVToDelete(
      wrappedRow: WrappedRow,
      physicalTableId: Long): WrappedEncodedRow = {
    val (encodedKey, encodedValue) = (
      new SerializableKey(RowKey.toRowKey(physicalTableId, wrappedRow.handle).getBytes),
      new Array[Byte](0))
    WrappedEncodedRow(
      wrappedRow.row,
      wrappedRow.handle,
      encodedKey,
      encodedValue,
      isIndex = false,
      -1,
      remove = true)
  }

  /**
   * use all indices to generate Index kv.
   * For isCommonHandle, we exclude primary key for it has been built by record
   * For isPkHandle, we don't do this because primary key is not included in indices
   *
   * @param rdd
   * @param remove
   * @param tiTable
   * @return Map[Long, RDD[WrappedEncodedRow], The key of map is indexId
   */
  def generateIndexKVRDDs(
      rdd: RDD[WrappedRow],
      tiTable: TableCommon,
      remove: Boolean): Map[Long, RDD[WrappedEncodedRow]] = {
    val tableInfo = tiTable.getTableInfo
    tableInfo.getIndices.asScala.flatMap { index =>
      if (tableInfo.isCommonHandle && index.isPrimary) {
        None
      } else {
        Some((index.getId, generateIndexRDD(rdd, index, tiTable, remove)))
      }
    }.toMap
  }

  def generateIndexKVs(
      rdd: WrappedRow,
      tiTable: TableCommon,
      remove: Boolean): mutable.Map[Long, mutable.Set[WrappedEncodedRow]] = {
    val tableInfo = tiTable.getTableInfo
    listPair2Multimap(tableInfo.getIndices.asScala.flatMap { index =>
      if (tableInfo.isCommonHandle && index.isPrimary) {
        None
      } else {
        Some((index.getId, generateIndex(rdd, index, tiTable, remove)))
      }
    }.toList)
  }

  /**
   * mix the results that are produced by method generateIndexKVs
   *
   * @param sc
   * @param rdd
   * @param TableCommon
   * @param remove
   * @return
   */
  def generateIndexKVRDD(
      sc: SparkContext,
      rdd: RDD[WrappedRow],
      tiTable: TableCommon,
      remove: Boolean): RDD[WrappedEncodedRow] = {
    val rdds = generateIndexKVRDDs(rdd, tiTable, remove)
    rdds.values.foldLeft(sc.emptyRDD[WrappedEncodedRow])(_ ++ _)
  }

  def generateIndexKV(
      rdd: WrappedRow,
      tiTable: TableCommon,
      remove: Boolean): List[WrappedEncodedRow] = {
    val rdds = generateIndexKVs(rdd, tiTable, remove)
    rdds.values.flatten.toList
  }

  /**
   * generateIndex for UniqueIndexKey and SecondaryIndexKey
   */
  private def generateIndexRDD(
      rdd: RDD[WrappedRow],
      index: TiIndexInfo,
      tiTable: TableCommon,
      remove: Boolean): RDD[WrappedEncodedRow] = {
    rdd.map { row =>
      generateIndex(row, index, tiTable, remove)
    }
  }

  private def generateIndex(
      row: WrappedRow,
      index: TiIndexInfo,
      tiTable: TableCommon,
      remove: Boolean) = {
    val (encodedKey, encodedValue) =
      generateIndexKeyAndValue(row.row, row.handle, index, tiTable, remove)
    WrappedEncodedRow(
      row.row,
      row.handle,
      encodedKey,
      encodedValue,
      isIndex = true,
      index.getId,
      remove)
  }

  /**
   * construct unique index and non-unique index and value to be inserted into TiKV
   * NOTE:
   *      pk is not handle case is equivalent to unique index.
   *      for non-unique index, handle will be encoded as part of index key. In contrast, unique
   *      index encoded handle to value.
   */
  private def generateIndexKeyAndValue(
      row: TiRow,
      handle: Handle,
      index: TiIndexInfo,
      tiTable: TableCommon,
      remove: Boolean): (SerializableKey, Array[Byte]) = {
    val encodeIndexResult =
      IndexKey.genIndexKey(locatePhysicalTable(tiTable), row, index, handle, tiTable.getTableInfo)

    val value = if (remove) {
      new Array[Byte](0)
    } else {
      TableCodec.genIndexValue(
        row,
        handle,
        tiTable.getTableInfo.getCommonHandleVersion,
        encodeIndexResult.distinct,
        index,
        tiTable.getTableInfo)
    }

    (new SerializableKey(encodeIndexResult.indexKey), value)
  }

  /**
   * @param TableCommon
   * @return
   */
  def locatePhysicalTable(tiTable: TableCommon): Long = {
    tiTable.getPhysicalTableId
  }

  /**
   * Convert a list of (key, value) pairs to a map from key to a set of values.
   * @param list the list of (key, value) pairs
   * @tparam A key type
   * @tparam B value type
   * @return
   */
  def listPair2Multimap[A, B](list: List[(A, B)]) =
    list.foldLeft(new mutable.HashMap[A, mutable.Set[B]] with mutable.MultiMap[A, B]) {
      (acc, pair) => acc.addBinding(pair._1, pair._2)
    }
}
