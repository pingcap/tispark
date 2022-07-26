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

package com.pingcap.tispark.write

import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tikv.partition.{PartitionedTable, TableCommon}
import com.pingcap.tikv.{ClientSession, TiConfiguration}
import com.pingcap.tispark.utils.TiUtil.sparkConfToTiConf
import com.pingcap.tispark.utils.{SchemaUpdateTime, TwoPhaseCommitHepler, WriteUtil}
import com.pingcap.tispark.write.TiBatchWrite.TiRow
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory

case class TiDBDelete(
    df: DataFrame,
    database: String,
    tableName: String,
    startTs: Long,
    tiDBOptions: Option[TiDBOptions] = None) {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  @transient lazy val tiSession: ClientSession = getTiSessionFromSparkConf

  // Call copyTableWithRowId to
  // 1.match the schema of dataframe
  // 2.make extract handle more convenience for (pkIsHandle || isCommonHandle) is always true.
  val tiTableInfo: TiTableInfo =
    tiSession.getCatalog.getTable(database, tableName).copyTableWithRowId()

  @transient private var persistedDFList: List[DataFrame] = Nil
  @transient private var persistedRDDList: List[RDD[_]] = Nil

  def delete(): Unit = {

    //check
    check(df)

    //check empty
    if (df.rdd.isEmpty()) {
      logger.info("DELETE with empty data")
      return
    }

    val colsInDf = df.columns.toList.map(_.toLowerCase())

    /**
     * There will be a stuck the following codes are executed after df.persist(),
     * so we use groupByKey() to trigger action firstly.
     * The reason why getting stuck has not been figured out yet.
     *
     * TableCommon is the physical table associated with the TiRow, we can get logical table name
     * from tiTableInfo.
     * - if the table is partitioned, we need to transfer the logical table to the physical table
     * and then group the rows by the physical table.
     * - if the table is not partitioned, the logical table is the same as the physical table.
     */
    val tiRowMapRDD: RDD[(TableCommon, Iterable[TiRow])] =
      df.rdd
        .mapPartitions { partition =>
          // Since using mapPartitions and making table construction out of rowIterator logic,
          // each partition will construct a new logical table and associated physical tables.
          val table = new TableCommon(tiTableInfo.getId, tiTableInfo.getId, tiTableInfo)

          if (tiTableInfo.isPartitionEnabled) {
            val pTable = PartitionedTable.newPartitionTable(table, tiTableInfo)
            partition.map { row =>
              val tiRow = WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf)
              // locate partition and return the physical table
              pTable.locatePartition(tiRow) -> tiRow
            }
          } else {
            partition.map { row =>
              //Convert Spark row to TiKV row
              val tiRow = WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf)
              table -> tiRow
            }
          }
        }
        .groupByKey()

    //persistDF
    val persistDf = df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedDFList = persistDf :: persistedDFList

    //Extract handle
    val deletionMapRDD: RDD[(TableCommon, Iterable[WrappedRow])] = tiRowMapRDD.mapValues {
      case tiRow =>
        tiRow.map { row => WrappedRow(row, WriteUtil.extractHandle(row, tiTableInfo)) }
    }

    // encode record & index
    val recordKVRDD: RDD[WrappedEncodedRow] = deletionMapRDD.flatMap {
      case (table, wrappedRow) =>
        wrappedRow.map { row =>
          WriteUtil.generateRecordKVToDelete(row, table.getPhysicalTableId)
        }
    }

    val indexKVRDD: RDD[WrappedEncodedRow] = deletionMapRDD.flatMap {
      case (table, wrappedEncodedRow) =>
        wrappedEncodedRow.flatMap { row =>
          WriteUtil.generateIndexKV(row, table, remove = true)
        }
    }

    val keyValueRDD = (recordKVRDD ++ indexKVRDD).map(obj => (obj.encodedKey, obj.encodedValue))

    //persist KeyValueRDD
    val persistKeyValueRDD =
      keyValueRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedRDDList = persistKeyValueRDD :: persistedRDDList

    // 2PC
    val twoPhaseCommitHelper =
      if (tiDBOptions.isEmpty) new TwoPhaseCommitHepler(startTs)
      else new TwoPhaseCommitHepler(startTs, tiDBOptions.get)
    try {
      // take one row as primary key
      val (primaryKey: SerializableKey, primaryRow: Array[Byte]) = {
        val takeOne = persistKeyValueRDD.take(1)
        takeOne(0)
      }

      // filter primary key
      val secondaryKeysRDD = persistKeyValueRDD.filter { keyValue =>
        !keyValue._1.equals(primaryKey)
      }

      // 2PC
      twoPhaseCommitHelper.prewritePrimaryKeyByDriver(primaryKey, primaryRow)
      twoPhaseCommitHelper.prewriteSecondaryKeyByExecutors(secondaryKeysRDD, primaryKey)
      val commitTs = twoPhaseCommitHelper.commitPrimaryKeyWithRetryByDriver(
        primaryKey,
        List(SchemaUpdateTime(database, tableName, tiTableInfo.getUpdateTimestamp)))
      twoPhaseCommitHelper.stopPrimaryKeyTTLUpdate()
      twoPhaseCommitHelper.commitSecondaryKeyByExecutors(secondaryKeysRDD, commitTs)
    } finally {
      twoPhaseCommitHelper.close()
    }
  }

  def unpersistAll(): Unit = {
    persistedDFList.foreach(_.unpersist())
    persistedRDDList.foreach(_.unpersist())
  }

  /**
   * check unsupport
   * check columns
   * check pkIsHandle and isCommonHandle
   *
   * @param df
   * @throws IllegalArgumentException if check fail
   */
  private def check(df: DataFrame): Unit = {
    // Only RangePartition and HashPartition are supported
    if (tiTableInfo.isPartitionEnabled) {
      val pType = tiTableInfo.getPartitionInfo.getType
      if (pType != PartitionType.RangePartition && pType != PartitionType.HashPartition) {
        throw new UnsupportedOperationException(s"Unsupported partition type: $pType")
      }
    }

    // Check columns: Defensive programming, it won't happen in theory
    if (df.columns.length != tiTableInfo.getColumns.size()) {
      throw new IllegalArgumentException(
        s"TiSpark Delete Unknown Error: data col size != table column size")
    }

    // Check pkIsHandle and isCommonHandle: Defensive programming , it won't happen in theory
    // table will be isPkHandle after call copyTableWithRowId if it is not isPkHandle or isCommonHandle
    if (!tiTableInfo.isPkHandle && !tiTableInfo.isCommonHandle) {
      throw new IllegalArgumentException(
        s"TiSpark Delete Unknown Error: isPkHandle or isCommonHandle after copyTableWithRowId")
    }
  }

  private def getTiSessionFromSparkConf: ClientSession = {
    val sparkConf: SparkConf = SparkContext.getOrCreate().getConf
    val tiConf: TiConfiguration = sparkConfToTiConf(sparkConf, Option.empty)
    ClientSession.getInstance(tiConf)
  }

}
