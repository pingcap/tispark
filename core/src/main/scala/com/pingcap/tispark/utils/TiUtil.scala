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

package com.pingcap.tispark.utils

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tikv.datatype.TypeMapping
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo, TiTimestamp}
import com.pingcap.tikv.region.TiStoreType
import com.pingcap.tikv.types._
import com.pingcap.tispark._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.slf4j.LoggerFactory
import org.tikv.kvproto.Kvrpcpb.{CommandPri, IsolationLevel}

import java.time.{Instant, LocalDate, ZoneId}
import java.util.TimeZone
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeUnit.NANOSECONDS

object TiUtil {
  private final val logger = LoggerFactory.getLogger(getClass.getName)
  val MICROS_PER_MILLIS = 1000L
  val MICROS_PER_SECOND = 1000000L

  def defaultTimeZone(): TimeZone = TimeZone.getDefault

  def daysToMillis(days: Int): Long = {
    daysToMillis(days, defaultTimeZone().toZoneId)
  }

  def daysToMillis(days: Int, zoneId: ZoneId): Long = {
    val instant = daysToLocalDate(days).atStartOfDay(zoneId).toInstant
    toMillis(instantToMicros(instant))
  }

  /*
   * Converts the timestamp to milliseconds since epoch. In spark timestamp values have microseconds
   * precision, so this conversion is lossy.
   */
  def toMillis(us: Long): Long = {
    // When the timestamp is negative i.e before 1970, we need to adjust the millseconds portion.
    // Example - 1965-01-01 10:11:12.123456 is represented as (-157700927876544) in micro precision.
    // In millis precision the above needs to be represented as (-157700927877).
    Math.floorDiv(us, MICROS_PER_MILLIS)
  }

  def instantToMicros(instant: Instant): Long = {
    val us = Math.multiplyExact(instant.getEpochSecond, MICROS_PER_SECOND)
    val result = Math.addExact(us, NANOSECONDS.toMicros(instant.getNano))
    result
  }

  def daysToLocalDate(days: Int): LocalDate = LocalDate.ofEpochDay(days)

  def getSchemaFromTable(table: TiTableInfo): StructType = {
    val fields = new Array[StructField](table.getColumns.size())
    for (i <- 0 until table.getColumns.size()) {
      val col = table.getColumns.get(i)
      val notNull = col.getType.isNotNull
      val metadata = new MetadataBuilder()
        .putString("name", col.getName)
        .build()
      fields(i) = StructField(
        col.getName,
        TypeMapping.toSparkType(col.getType),
        nullable = !notNull,
        metadata)
    }
    new StructType(fields)
  }

  def isDataFrameEmpty(df: DataFrame): Boolean = {
    df.rdd.isEmpty()
  }

  def sparkConfToTiConf(conf: SparkConf, option: Option[String]): TiConfiguration = {
    val tiConf = TiConfiguration.createDefault(if (option.isDefined) {
      option.get
    } else conf.get(TiConfigConst.PD_ADDRESSES))

    sparkConfToTiConfWithoutPD(conf, tiConf)
  }

  def sparkConfToTiConfWithoutPD(conf: SparkConf, tiConf: TiConfiguration): TiConfiguration = {
    if (conf.contains(TiConfigConst.GRPC_FRAME_SIZE)) {
      tiConf.setMaxFrameSize(conf.get(TiConfigConst.GRPC_FRAME_SIZE).toInt)
    }

    if (conf.contains(TiConfigConst.GRPC_TIMEOUT)) {
      tiConf.setTimeout(conf.get(TiConfigConst.GRPC_TIMEOUT).toInt)
      tiConf.setTimeoutUnit(TimeUnit.SECONDS)
    }

    if (conf.contains(TiConfigConst.INDEX_SCAN_BATCH_SIZE)) {
      tiConf.setIndexScanBatchSize(conf.get(TiConfigConst.INDEX_SCAN_BATCH_SIZE).toInt)
    }

    if (conf.contains(TiConfigConst.INDEX_SCAN_CONCURRENCY)) {
      tiConf.setIndexScanConcurrency(conf.get(TiConfigConst.INDEX_SCAN_CONCURRENCY).toInt)
    }

    if (conf.contains(TiConfigConst.TABLE_SCAN_CONCURRENCY)) {
      tiConf.setTableScanConcurrency(conf.get(TiConfigConst.TABLE_SCAN_CONCURRENCY).toInt)
    }

    if (conf.contains(TiConfigConst.REQUEST_ISOLATION_LEVEL)) {
      val isolationLevel = conf.get(TiConfigConst.REQUEST_ISOLATION_LEVEL)
      if (isolationLevel.equals(TiConfigConst.SNAPSHOT_ISOLATION_LEVEL)) {
        tiConf.setIsolationLevel(IsolationLevel.SI)
      } else {
        tiConf.setIsolationLevel(IsolationLevel.RC)
      }
    }

    if (conf.contains(TiConfigConst.REQUEST_COMMAND_PRIORITY)) {
      val priority =
        CommandPri.valueOf(conf.get(TiConfigConst.REQUEST_COMMAND_PRIORITY))
      tiConf.setCommandPriority(priority)
    }

    if (conf.contains(TiConfigConst.SHOW_ROWID)) {
      tiConf.setShowRowId(conf.get(TiConfigConst.SHOW_ROWID).toBoolean)
    }

    if (conf.contains(TiConfigConst.DB_PREFIX)) {
      tiConf.setDBPrefix(conf.get(TiConfigConst.DB_PREFIX))
    }

    if (conf.contains(TiConfigConst.WRITE_ENABLE)) {
      tiConf.setWriteEnable(conf.get(TiConfigConst.WRITE_ENABLE).toBoolean)
    }

    if (conf.contains(TiConfigConst.WRITE_WITHOUT_LOCK_TABLE)) {
      tiConf.setWriteWithoutLockTable(conf.get(TiConfigConst.WRITE_WITHOUT_LOCK_TABLE).toBoolean)
    }

    if (conf.contains(TiConfigConst.WRITE_ALLOW_SPARK_SQL)) {
      tiConf.setWriteAllowSparkSQL(conf.get(TiConfigConst.WRITE_ALLOW_SPARK_SQL).toBoolean)
    }

    if (conf.contains(TiConfigConst.TIKV_REGION_SPLIT_SIZE_IN_MB)) {
      tiConf.setTikvRegionSplitSizeInMB(
        conf.get(TiConfigConst.TIKV_REGION_SPLIT_SIZE_IN_MB).toInt)
    }

    if (conf.contains(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD)) {
      tiConf.setDowngradeThreshold(
        conf.get(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD).toInt)
    }

    if (conf.contains(TiConfigConst.PARTITION_PER_SPLIT)) {
      tiConf.setPartitionPerSplit(conf.get(TiConfigConst.PARTITION_PER_SPLIT).toInt)
    }

    if (conf.contains(TiConfigConst.ISOLATION_READ_ENGINES)) {
      import scala.collection.JavaConversions._
      tiConf.setIsolationReadEngines(
        getIsolationReadEnginesFromString(conf.get(TiConfigConst.ISOLATION_READ_ENGINES)).toList)
    }

    if (conf.contains(TiConfigConst.KV_CLIENT_CONCURRENCY)) {
      tiConf.setKvClientConcurrency(conf.get(TiConfigConst.KV_CLIENT_CONCURRENCY).toInt)
    }

    // TLS
    if (conf.contains(TiConfigConst.TIKV_TLS_ENABLE)) {
      tiConf.setTlsEnable(conf.get(TiConfigConst.TIKV_TLS_ENABLE).toBoolean)
    }

    if (conf.contains(TiConfigConst.TIKV_TRUST_CERT_COLLECTION)) {
      tiConf.setTrustCertCollectionFile(conf.get(TiConfigConst.TIKV_TRUST_CERT_COLLECTION))
    }

    if (conf.contains(TiConfigConst.TIKV_KEY_CERT_CHAIN)) {
      tiConf.setKeyCertChainFile(conf.get(TiConfigConst.TIKV_KEY_CERT_CHAIN))
    }

    if (conf.contains(TiConfigConst.TIKV_KEY_FILE)) {
      tiConf.setKeyFile(conf.get(TiConfigConst.TIKV_KEY_FILE))
    }

    tiConf
  }

  private def getIsolationReadEnginesFromString(str: String): List[TiStoreType] = {
    str
      .toLowerCase()
      .split(",")
      .map {
        case TiConfigConst.TIKV_STORAGE_ENGINE => TiStoreType.TiKV
        case TiConfigConst.TIFLASH_STORAGE_ENGINE => TiStoreType.TiFlash
        case s =>
          throw new UnsupportedOperationException(
            s"Unknown isolation engine type: $s, valid types are 'tikv, tiflash'")
      }
      .toList
  }

  def getChunkBatchSize(sqlContext: SQLContext): Int =
    sqlContext.getConf(TiConfigConst.CHUNK_BATCH_SIZE, "1024").toInt

  def getPartitionPerSplit(sqlContext: SQLContext): Int =
    sqlContext.getConf(TiConfigConst.PARTITION_PER_SPLIT, "10").toInt

  def getIsolationReadEngines(sqlContext: SQLContext): List[TiStoreType] =
    getIsolationReadEnginesFromString(
      sqlContext
        .getConf(TiConfigConst.ISOLATION_READ_ENGINES, TiConfigConst.DEFAULT_STORAGE_ENGINES))

  def registerUDFs(sparkSession: SparkSession): Unit = {
    val timeZoneStr: String = "TimeZone: " + Converter.getLocalTimezone.toString

    sparkSession.udf.register(
      "ti_version",
      () => {
        s"${TiSparkVersion.version}\n${TiSparkInfo.info}\n$timeZoneStr"
      })
    sparkSession.udf.register(
      "time_to_str",
      (value: Long, frac: Int) => Converter.convertDurationToStr(value, frac))
    sparkSession.udf
      .register("str_to_time", (value: String) => Converter.convertStrToDuration(value))
  }

  def getReqEstCountStr(req: TiDAGRequest): String =
    if (req.getEstimatedCount > 0) {
      import java.text.DecimalFormat
      val df = new DecimalFormat("#.#")
      s" EstimatedCount:${df.format(req.getEstimatedCount)}"
    } else ""

  def rowToInternalRow(
      row: Row,
      outputTypes: Seq[sql.types.DataType],
      converters: Seq[Any => Any]): InternalRow = {
    val mutableRow = new GenericInternalRow(outputTypes.length)
    for (i <- outputTypes.indices) {
      mutableRow(i) = converters(i)(row(i))
    }

    mutableRow
  }

  def getTiDBSnapshot(sparkSession: SparkSession): Option[TiTimestamp] = {
    val staleReadTs =
      sparkSession.conf.get(TiConfigConst.STALE_READ, TiConfigConst.DEFAULT_STALE_READ)
    logger.info(s"${TiConfigConst.STALE_READ} = $staleReadTs")
    if (staleReadTs.isEmpty) {
      Option.empty
    } else {
      Some(parseTimestamp(staleReadTs))
    }
  }

  private def parseTimestamp(str: String): TiTimestamp = {
    if (!isValidTimestampMill(str)) {
      throw new IllegalArgumentException(
        "Invalid value of " + TiConfigConst.STALE_READ + ":" + str)
    } else {
      try {
        val ts = java.lang.Long.parseLong(str)
        new TiTimestamp(ts, 0L)
      } catch {
        case _: Throwable =>
          throw new IllegalArgumentException(
            "Unknown error to parse" + TiConfigConst.STALE_READ + ":" + str)
      }
    }
  }

  private def isValidTimestampMill(str: String): Boolean = {
    if (StringUtils.isBlank(str) || !StringUtils.isNumeric(str)) {
      return false
    }
    str.length() == 13;
  }
}
