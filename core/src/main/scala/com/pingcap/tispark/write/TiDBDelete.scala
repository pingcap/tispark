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

import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tispark.TiConfigConst
import com.pingcap.tispark.utils.{SchemaUpdateTime, TwoPhaseCommitHepler, WriteUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.LoggerFactory

case class TiDBDelete(
    df: DataFrame,
    database: String,
    table: String,
    tiDBOptions: Option[TiDBOptions] = None) {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  @transient lazy val tiSession: TiSession = getTiSessionFromSparkConf

  // Call copyTableWithRowId to
  // 1.match the schema of dataframe
  // 2.make extract handle more convenience for (pkIsHandle || isCommonHandle) is always true.
  val tiTableInfo: TiTableInfo =
    tiSession.getCatalog.getTable(database, table).copyTableWithRowId()

  @transient private var persistedDFList: List[DataFrame] = Nil
  @transient private var persistedRDDList: List[RDD[_]] = Nil

  def delete(): Unit = {
    //persistDF
    val persistDf = df.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedDFList = persistDf :: persistedDFList

    //check
    check(persistDf)

    //check empty
    if (df.rdd.isEmpty()) {
      logger.info("DELETE with empty data")
      return
    }

    // get start_ts
    val startTs = tiSession.getTimestamp.getVersion
    logger.info(s"startTS: $startTs")

    //Convert Spark row to TiKV row
    val colsInDf = persistDf.columns.toList.map(_.toLowerCase())
    val tiRowRdd = persistDf.rdd.map(row => {
      WriteUtil.sparkRow2TiKVRow(row, tiTableInfo, colsInDf)
    })

    //Extract handle
    val deletion: RDD[WrappedRow] = tiRowRdd.map { row =>
      WrappedRow(row, WriteUtil.extractHandle(row, tiTableInfo))
    }

    //persist deletionRDD
    val persistDeletionRDD =
      deletion.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedRDDList = persistDeletionRDD :: persistedRDDList

    // encode record & index
    val tableId = tiTableInfo.getId
    val recordKV = WriteUtil.generateRecordKVToDelete(persistDeletionRDD, tableId)
    val indexKV = WriteUtil.generateIndexKV(
      SparkSession.active.sparkContext,
      persistDeletionRDD,
      tiTableInfo,
      remove = true)
    val keyValueRDD = (recordKV ++ indexKV).map(obj => (obj.encodedKey, obj.encodedValue))

    //persist KeyValueRDD
    val persistKeyValueRDD =
      keyValueRDD.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
    persistedRDDList = persistKeyValueRDD :: persistedRDDList

    //2PC
    val twoPhaseCommitHepler =
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
      twoPhaseCommitHepler.prewritePrimaryKeyByDriver(primaryKey, primaryRow)
      twoPhaseCommitHepler.prewriteSecondaryKeyByExecutors(secondaryKeysRDD, primaryKey)
      val commitTs = twoPhaseCommitHepler.commitPrimaryKeyWithRetryByDriver(
        primaryKey,
        List(SchemaUpdateTime(database, table, tiTableInfo.getUpdateTimestamp)))
      twoPhaseCommitHepler.commitSecondaryKeyByExecutors(secondaryKeysRDD, commitTs)
    } finally {
      twoPhaseCommitHepler.close()
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
   * @param df
   * @throws IllegalArgumentException if check fail
   */
  private def check(df: DataFrame): Unit = {
    // Delete from partition table is not supported
    if (tiTableInfo.isPartitionEnabled) {
      throw new IllegalArgumentException(
        "TiSpark currently does not support delete data from partition table!")
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

  private def getTiSessionFromSparkConf: TiSession = {
    val sparkConf: SparkConf = SparkContext.getOrCreate().getConf
    val tiConf: TiConfiguration =
      TiConfiguration.createDefault(sparkConf.get(TiConfigConst.PD_ADDRESSES))
    TiSession.getInstance(tiConf)
  }

}
