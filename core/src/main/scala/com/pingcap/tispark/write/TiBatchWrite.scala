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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.write

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv._
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.utils.{TiUtil, TwoPhaseCommitHepler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{DataFrame, SparkSession, TiContext, TiExtensions}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._

object TiBatchWrite {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType
  // Milliseconds
  private val MIN_DELAY_CLEAN_TABLE_LOCK = 60000
  private val DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA = 30000
  private val PRIMARY_KEY_COMMIT_BACKOFF =
    MIN_DELAY_CLEAN_TABLE_LOCK - DELAY_CLEAN_TABLE_LOCK_AND_COMMIT_BACKOFF_DELTA

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def write(df: DataFrame, tiContext: TiContext, options: TiDBOptions): Unit = {
    val dataToWrite = Map(DBTable(options.database, options.table) -> df)
    new TiBatchWrite(dataToWrite, tiContext, options).write()
  }

  @throws(classOf[NoSuchTableException])
  @throws(classOf[TiBatchWriteException])
  def write(
      dataToWrite: Map[DBTable, DataFrame],
      sparkSession: SparkSession,
      parameters: Map[String, String]): Unit = {
    TiExtensions.getTiContext(sparkSession) match {
      case Some(tiContext) =>
        new TiBatchWrite(
          dataToWrite,
          tiContext,
          new TiDBOptions(parameters ++ Map(TiDBOptions.TIDB_MULTI_TABLES -> "true"))).write()
      case None =>
        throw new TiBatchWriteException("TiExtensions is disable!")
    }
  }
}

class TiBatchWrite(
    @transient val dataToWrite: Map[DBTable, DataFrame],
    @transient val tiContext: TiContext,
    options: TiDBOptions)
    extends Serializable {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  import com.pingcap.tispark.write.TiBatchWrite._

  private var tiConf: TiConfiguration = _
  @transient private var tiSession: TiSession = _
  private var useTableLock: Boolean = _
  @transient private var ttlManager: TTLManager = _
  private var isTTLUpdate: Boolean = _
  private var lockTTLSeconds: Long = _
  @transient private var tiDBJDBCClient: TiDBJDBCClient = _
  @transient private var tiBatchWriteTables: List[TiBatchWriteTable] = _
  @transient private var startMS: Long = _
  private var startTs: Long = _
  private val twoPhaseCommitHepler: TwoPhaseCommitHepler = TwoPhaseCommitHepler(startTs, options)

  private def write(): Unit = {
    try {
      doWrite()
    } finally {
      close()
    }
  }

  private def close(): Unit = {
    try {
      if (tiBatchWriteTables != null) {
        tiBatchWriteTables.foreach(_.unlockTable())
      }
    } catch {
      case _: Throwable =>
    }

    try {
      if (tiBatchWriteTables != null) {
        tiBatchWriteTables.foreach(_.unpersistAll())
      }
    } catch {
      case _: Throwable =>
    }

    try {
      twoPhaseCommitHepler.close()
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

  private def doWrite(): Unit = {
    startMS = System.currentTimeMillis()

    // check if write enable
    if (!tiContext.tiConf.isWriteEnable) {
      throw new TiBatchWriteException(
        "tispark batch write is disabled! set spark.tispark.write.enable to enable.")
    }

    // initialize
    tiConf = mergeSparkConfWithDataSourceConf(tiContext.conf, options)
    tiSession = tiContext.tiSession
    val tikvSupportUpdateTTL = StoreVersion.minTiKVVersion("3.0.5", tiSession.getPDClient)
    val isTiDBV4 = StoreVersion.minTiKVVersion("4.0.0", tiSession.getPDClient)
    isTTLUpdate = options.isTTLUpdate(tikvSupportUpdateTTL)
    lockTTLSeconds = options.getLockTTLSeconds(tikvSupportUpdateTTL)
    tiDBJDBCClient = new TiDBJDBCClient(TiDBUtils.createConnectionFactory(options.url)())

    // init tiBatchWriteTables
    tiBatchWriteTables = {
      dataToWrite.map {
        case (dbTable, df) =>
          new TiBatchWriteTable(
            df,
            tiContext,
            options.setDBTable(dbTable),
            tiConf,
            tiDBJDBCClient,
            isTiDBV4)
      }.toList
    }

    // check unsupported
    tiBatchWriteTables.foreach(_.checkUnsupported())

    // cache data
    tiBatchWriteTables.foreach(_.persist())

    // check empty
    var allEmpty = true
    tiBatchWriteTables.foreach { table =>
      if (!table.isDFEmpty) {
        allEmpty = false
      }
    }
    if (allEmpty) {
      logger.warn("data is empty!")
      return
    }

    // lock table
    useTableLock = getUseTableLock
    if (useTableLock) {
      tiBatchWriteTables.foreach(_.lockTable())
    } else {
      if (!isTiDBV4) {
        if (tiContext.tiConf.isWriteWithoutLockTable) {
          logger.warn("write tidb-2.x or 3.x without lock table enabled! only for test!")
        } else {
          throw new TiBatchWriteException(
            "current tidb does not support LockTable or is disabled!")
        }
      }
    }

    // check schema
    tiBatchWriteTables.foreach(_.checkColumnNumbers())

    // get timestamp as start_ts
    val startTimeStamp = tiSession.getTimestamp
    startTs = startTimeStamp.getVersion
    logger.info(s"startTS: $startTs")

    // pre calculate
    val shuffledRDD: RDD[(SerializableKey, Array[Byte])] = {
      val rddList = tiBatchWriteTables.map(_.preCalculate(startTimeStamp))
      if (rddList.lengthCompare(1) == 0) {
        rddList.head
      } else {
        tiContext.sparkSession.sparkContext.union(rddList)
      }
    }

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

    logger.info(s"primary key: $primaryKey")

    // split region
    val finalRDD = if (options.enableRegionSplit) {
      val insertRDD = shuffledRDD.filter(kv => kv._2.length > 0)
      val orderedSplitPoints = getRegionSplitPoints(insertRDD)

      try {
        tiSession.splitRegionAndScatter(
          orderedSplitPoints.map(_.bytes).asJava,
          options.splitRegionBackoffMS,
          options.scatterRegionBackoffMS,
          options.scatterWaitMS)
      } catch {
        case e: Throwable => logger.warn("split region and scatter error!", e)
      }

      // shuffle according to split points
      shuffledRDD.partitionBy(
        new TiReginSplitPartitioner(orderedSplitPoints, options.maxWriteTaskNumber))
    } else {
      shuffledRDD
    }

    // filter primary key
    val secondaryKeysRDD = finalRDD.filter { keyValue =>
      !keyValue._1.equals(primaryKey)
    }

    // for test
    if (options.sleepBeforePrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepBeforePrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepBeforePrewritePrimaryKey)
    }

    // driver primary pre-write
    twoPhaseCommitHepler.prewritePrimaryKeyByDriver(primaryKey, primaryRow)

    // for test
    if (options.sleepAfterPrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepAfterPrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepAfterPrewritePrimaryKey)
    }

    // executors secondary pre-write
    twoPhaseCommitHepler.prewriteSecondaryKeyByExecutors(secondaryKeysRDD, primaryKey)

    // check connection lost if using lock table
    checkConnectionLost()
    // checkschema if not useTableLock
    val schemaUpdateTimes = if (useTableLock) {
      Nil
    } else {
      tiBatchWriteTables.map(_.buildSchemaUpdateTime())
    }
    // driver primary commit
    val commitTs =
      twoPhaseCommitHepler.commitPrimaryKeyWithRetryByDriver(primaryKey, schemaUpdateTimes)

    // unlock table
    tiBatchWriteTables.foreach(_.unlockTable())

    // executors secondary commit
    twoPhaseCommitHepler.commitSecondaryKeyByExecutors(secondaryKeysRDD, commitTs)

    // update table statistics: modify_count & count
    if (options.enableUpdateTableStatistics) {
      tiBatchWriteTables.foreach(_.updateTableStatistics(startTs))
    }

    val endMS = System.currentTimeMillis()
    logger.info(s"batch write cost ${(endMS - startMS) / 1000} seconds")
  }

  private def getRegionSplitPoints(
      rdd: RDD[(SerializableKey, Array[Byte])]): List[SerializableKey] = {
    val count = rdd.count()

    if (count < options.regionSplitThreshold) {
      return Nil
    }

    val regionSplitPointNum = if (options.regionSplitNum > 0) {
      options.regionSplitNum
    } else {
      Math.min(
        Math.max(
          options.minRegionSplitNum,
          Math.ceil(count.toDouble / options.regionSplitKeys).toInt),
        options.maxRegionSplitNum)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * options.sampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData = rdd.sample(false, sampleSize.toDouble / count).collect()
    logger.info(s"sampleData size=${sampleData.length}")

    val splitPointNumUsingSize = if (options.regionSplitUsingSize) {
      val avgSize = getAverageSizeInBytes(sampleData)
      logger.info(s"avgSize=$avgSize Bytes")
      if (avgSize <= options.bytesPerRegion / options.regionSplitKeys) {
        regionSplitPointNum
      } else {
        Math.min(
          Math.floor((count.toDouble / options.bytesPerRegion) * avgSize).toInt,
          sampleData.length / 10)
      }
    } else {
      regionSplitPointNum
    }
    logger.info(s"splitPointNumUsingSize=$splitPointNumUsingSize")

    val finalRegionSplitPointNum = Math.min(
      Math.max(options.minRegionSplitNum, splitPointNumUsingSize),
      options.maxRegionSplitNum)
    logger.info(s"finalRegionSplitPointNum=$finalRegionSplitPointNum")

    val sortedSampleData = sampleData
      .map(_._1)
      .sorted(new Ordering[SerializableKey] {
        override def compare(x: SerializableKey, y: SerializableKey): Int = {
          x.compareTo(y)
        }
      })
    val orderedSplitPoints = new Array[SerializableKey](finalRegionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (finalRegionSplitPointNum + 1)).toInt
    for (i <- 0 until finalRegionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
  }

  private def getAverageSizeInBytes(keyValues: Array[(SerializableKey, Array[Byte])]): Int = {
    var avg: Double = 0
    var t: Int = 1
    keyValues.foreach { keyValue =>
      val keySize: Double = keyValue._1.bytes.length + keyValue._2.length
      avg = avg + (keySize - avg) / t
      t = t + 1
    }
    Math.ceil(avg).toInt
  }

  private def getUseTableLock: Boolean = {
    if (!options.useTableLock(StoreVersion.minTiKVVersion("4.0.0", tiSession.getPDClient))) {
      false
    } else {
      if (tiDBJDBCClient.isEnableTableLock) {
        if (tiDBJDBCClient.getDelayCleanTableLock >= MIN_DELAY_CLEAN_TABLE_LOCK) {
          true
        } else {
          logger.warn(
            s"table lock disabled! to enable table lock, please set tidb config: delay-clean-table-lock >= $MIN_DELAY_CLEAN_TABLE_LOCK")
          false
        }
      } else {
        false
      }
    }
  }

  private def mergeSparkConfWithDataSourceConf(
      conf: SparkConf,
      options: TiDBOptions): TiConfiguration = {
    val clonedConf = conf.clone()
    // priority: data source config > spark config
    clonedConf.setAll(options.parameters)
    TiUtil.sparkConfToTiConf(clonedConf, Option.empty)
  }

  private def checkConnectionLost(): Unit = {
    if (useTableLock) {
      if (tiDBJDBCClient.isClosed) {
        throw new TiBatchWriteException("tidb's jdbc connection is lost!")
      }
    }
  }
}
