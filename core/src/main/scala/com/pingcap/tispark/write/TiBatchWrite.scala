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

package com.pingcap.tispark.write

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tikv.util.{BackOffer, ConcreteBackOffer}
import com.pingcap.tikv.{TTLManager, TiDBJDBCClient, _}
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.utils.TiUtil
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
      if (ttlManager != null) {
        ttlManager.close()
      }
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
    isTTLUpdate = options.isTTLUpdate(tikvSupportUpdateTTL)
    lockTTLSeconds = options.getLockTTLSeconds(tikvSupportUpdateTTL)
    tiDBJDBCClient = new TiDBJDBCClient(
      TiDBUtils.createConnectionFactory(options.url)(),
      options.writeSplitRegionFinish)

    // init tiBatchWriteTables
    tiBatchWriteTables = {
      val isEnableSplitRegion = tiDBJDBCClient.isEnableSplitRegion
      dataToWrite.map {
        case (dbTable, df) =>
          new TiBatchWriteTable(
            df,
            tiContext,
            options.setDBTable(dbTable),
            tiConf,
            tiDBJDBCClient,
            isEnableSplitRegion)
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
      val isTiDBV4 = StoreVersion.minTiKVVersion("4.0.0", tiSession.getPDClient)
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
    val startTs = startTimeStamp.getVersion
    logger.info(s"startTS: $startTs")

    // pre calculate
    var shuffledRDD: RDD[(SerializableKey, Array[Byte])] = {
      val rddList = tiBatchWriteTables.map(_.preCalculate(startTimeStamp))
      if (rddList.lengthCompare(1) == 0) {
        rddList.head
      } else {
        tiContext.sparkSession.sparkContext.union(rddList)
      }
    }
    val shuffledRDDCount = shuffledRDD.count()
    logger.info(s"write kv data count=$shuffledRDDCount")

    if (options.enableRegionSplit && "v2".equals(options.regionSplitMethod)) {
      // calculate region split points
      val orderedSplitPoints = getRegionSplitPoints(shuffledRDD, shuffledRDDCount)

      // split region
      try {
        tiSession.splitRegionAndScatter(
          orderedSplitPoints.map(_.bytes).asJava,
          options.splitRegionBackoffMS,
          options.scatterWaitMS)
      } catch {
        case e: Throwable => logger.warn("split region and scatter error!", e)
      }

      // shuffle according to split points
      shuffledRDD = shuffledRDD.partitionBy(new TiReginSplitPartitioner(orderedSplitPoints))
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

    // filter primary key
    val secondaryKeysRDD = shuffledRDD.filter { keyValue =>
      !keyValue._1.equals(primaryKey)
    }

    // driver primary pre-write
    val ti2PCClient =
      new TwoPhaseCommitter(
        tiConf,
        startTs,
        lockTTLSeconds * 1000,
        options.txnPrewriteBatchSize,
        options.txnCommitBatchSize,
        options.writeBufferSize,
        options.writeThreadPerTask,
        options.retryCommitSecondaryKey)
    val prewritePrimaryBackoff =
      ConcreteBackOffer.newCustomBackOff(BackOffer.BATCH_PREWRITE_BACKOFF)
    logger.info("start to prewritePrimaryKey")
    ti2PCClient.prewritePrimaryKey(prewritePrimaryBackoff, primaryKey.bytes, primaryRow)
    logger.info("prewritePrimaryKey success")

    // for test
    if (options.sleepAfterPrewritePrimaryKey > 0) {
      logger.info(s"sleep ${options.sleepAfterPrewritePrimaryKey} ms for test")
      Thread.sleep(options.sleepAfterPrewritePrimaryKey)
    }

    // start primary key ttl update
    if (isTTLUpdate) {
      ttlManager = new TTLManager(tiConf, startTs, primaryKey.bytes)
      ttlManager.keepAlive()
    }

    // executors secondary pre-write
    logger.info("start to prewriteSecondaryKeys")
    secondaryKeysRDD.foreachPartition { iterator =>
      val ti2PCClientOnExecutor =
        new TwoPhaseCommitter(
          tiConf,
          startTs,
          lockTTLSeconds * 1000,
          options.txnPrewriteBatchSize,
          options.txnCommitBatchSize,
          options.writeBufferSize,
          options.writeThreadPerTask,
          options.retryCommitSecondaryKey)

      val pairs = iterator.map { keyValue =>
        new BytePairWrapper(keyValue._1.bytes, keyValue._2)
      }.asJava

      ti2PCClientOnExecutor.prewriteSecondaryKeys(
        primaryKey.bytes,
        pairs,
        options.prewriteBackOfferMS)

      try {
        ti2PCClientOnExecutor.close()
      } catch {
        case _: Throwable =>
      }
    }
    logger.info("prewriteSecondaryKeys success")

    // for test
    if (options.sleepAfterPrewriteSecondaryKey > 0) {
      logger.info(s"sleep ${options.sleepAfterPrewriteSecondaryKey} ms for test")
      Thread.sleep(options.sleepAfterPrewriteSecondaryKey)
    }

    // driver primary commit
    val commitTs = tiSession.getTimestamp.getVersion
    // check commitTS
    if (commitTs <= startTs) {
      throw new TiBatchWriteException(
        s"invalid transaction tso with startTs=$startTs, commitTs=$commitTs")
    }

    // check schema change
    if (!useTableLock) {
      tiBatchWriteTables.foreach(_.checkSchemaChange())
    }

    // for test
    if (options.sleepAfterGetCommitTS > 0) {
      logger.info(s"sleep ${options.sleepAfterGetCommitTS} ms for test")
      Thread.sleep(options.sleepAfterGetCommitTS)
    }

    val commitPrimaryBackoff = ConcreteBackOffer.newCustomBackOff(PRIMARY_KEY_COMMIT_BACKOFF)

    // check connection lost if using lock table
    checkConnectionLost()

    logger.info("start to commitPrimaryKey")
    ti2PCClient.commitPrimaryKey(commitPrimaryBackoff, primaryKey.bytes, commitTs)
    try {
      ti2PCClient.close()
    } catch {
      case _: Throwable =>
    }
    logger.info("commitPrimaryKey success")

    // stop primary key ttl update
    if (isTTLUpdate) {
      ttlManager.close()
    }

    // unlock table
    tiBatchWriteTables.foreach(_.unlockTable())

    // executors secondary commit
    if (!options.skipCommitSecondaryKey) {
      logger.info("start to commitSecondaryKeys")
      secondaryKeysRDD.foreachPartition { iterator =>
        val ti2PCClientOnExecutor = new TwoPhaseCommitter(
          tiConf,
          startTs,
          lockTTLSeconds * 1000,
          options.txnPrewriteBatchSize,
          options.txnCommitBatchSize,
          options.writeBufferSize,
          options.writeThreadPerTask,
          options.retryCommitSecondaryKey)

        val keys = iterator.map { keyValue =>
          new ByteWrapper(keyValue._1.bytes)
        }.asJava

        try {
          ti2PCClientOnExecutor.commitSecondaryKeys(keys, commitTs, options.commitBackOfferMS)
        } catch {
          case e: TiBatchWriteException =>
            // ignored
            logger.warn(s"commit secondary key error", e)
        }

        try {
          ti2PCClientOnExecutor.close()
        } catch {
          case _: Throwable =>
        }
      }
      logger.info("commitSecondaryKeys finish")
    } else {
      logger.info("skipping commit secondary key")
    }

    val endMS = System.currentTimeMillis()
    logger.info(s"batch write cost ${(endMS - startMS) / 1000} seconds")
  }

  private def getRegionSplitPoints(
      rdd: RDD[(SerializableKey, Array[Byte])],
      count: Long): List[SerializableKey] = {
    if (count < options.regionSplitThreshold) {
      return Nil
    }

    val regionSplitPointNum = if (options.regionSplitNum > 0) {
      options.regionSplitNum
    } else {
      Math.max(
        options.minRegionSplitNum,
        Math.ceil(count.toDouble / options.regionSplitKeys).toInt)
    }
    logger.info(s"regionSplitPointNum=$regionSplitPointNum")

    val sampleSize = (regionSplitPointNum + 1) * options.sampleSplitFrac
    logger.info(s"sampleSize=$sampleSize")

    val sampleData =
      rdd
        .map(_._1)
        .sample(withReplacement = false, fraction = sampleSize.toDouble / count)
        .collect()
    logger.info(s"sampleData size=${sampleData.length}")

    val sortedSampleData = sampleData.sorted(new Ordering[SerializableKey] {
      override def compare(x: SerializableKey, y: SerializableKey): Int = {
        x.compareTo(y)
      }
    })

    val orderedSplitPoints = new Array[SerializableKey](regionSplitPointNum)
    val step = Math.floor(sortedSampleData.length.toDouble / (regionSplitPointNum + 1)).toInt
    for (i <- 0 until regionSplitPointNum) {
      orderedSplitPoints(i) = sortedSampleData((i + 1) * step)
    }

    logger.info(s"orderedSplitPoints size=${orderedSplitPoints.length}")
    orderedSplitPoints.toList
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
    TiUtil.sparkConfToTiConf(clonedConf)
  }

  private def checkConnectionLost(): Unit = {
    if (useTableLock) {
      if (tiDBJDBCClient.isClosed) {
        throw new TiBatchWriteException("tidb's jdbc connection is lost!")
      }
    }
  }
}
