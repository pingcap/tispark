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

package com.pingcap.tispark.utils

import java.util.concurrent.TimeUnit

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tikv.datatype.TypeMapping
import com.pingcap.tikv.meta.{TiDAGRequest, TiTableInfo}
import com.pingcap.tikv.region.TiStoreType
import com.pingcap.tikv.types._
import com.pingcap.tispark.{TiConfigConst, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, sql}
import org.tikv.kvproto.Kvrpcpb.{CommandPri, IsolationLevel}

object TiUtil {
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

  def sparkConfToTiConf(conf: SparkConf): TiConfiguration = {
    val tiConf = TiConfiguration.createDefault(conf.get(TiConfigConst.PD_ADDRESSES))

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
      val priority = CommandPri.valueOf(conf.get(TiConfigConst.REQUEST_COMMAND_PRIORITY))
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
    sqlContext.getConf(TiConfigConst.PARTITION_PER_SPLIT, "1").toInt

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
}
