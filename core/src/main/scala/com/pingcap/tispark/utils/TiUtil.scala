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
import com.pingcap.tikv.types._
import com.pingcap.tispark.{TiConfigConst, _}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.{ColumnarCoprocessorRDD, ColumnarRegionTaskExec}
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.{sql, SparkConf}
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
        metadata
      )
    }
    new StructType(fields)
  }

  def isDataFrameEmpty(df: DataFrame): Boolean = {
    df.limit(1).count() == 0
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
      tiConf.setTikvRegionSplitSizeInMB(conf.get(TiConfigConst.TIKV_REGION_SPLIT_SIZE_IN_MB).toInt)
    }

    if (conf.contains(TiConfigConst.USE_TIFLASH)) {
      tiConf.setUseTiFlash(conf.get(TiConfigConst.USE_TIFLASH).toBoolean)
    }

    if (conf.contains(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD)) {
      tiConf.setDowngradeThreshold(
        conf.get(TiConfigConst.REGION_INDEX_SCAN_DOWNGRADE_THRESHOLD).toInt
      )
    }
    tiConf
  }

  def getChunkBatchSize(sqlContext: SQLContext) =
    sqlContext.getConf(TiConfigConst.CHUNK_BATCH_SIZE, "1024").toInt

  def registerUDFs(sparkSession: SparkSession): Unit = {
    val timeZoneStr: String = "TimeZone: " + Converter.getLocalTimezone.toString

    sparkSession.udf.register("ti_version", () => {
      s"${TiSparkVersion.version}\n${TiSparkInfo.info}\n$timeZoneStr"
    })
    sparkSession.udf.register(
      "time_to_str",
      (value: Long, frac: Int) => Converter.convertDurationToStr(value, frac)
    )
    sparkSession.udf
      .register("str_to_time", (value: String) => Converter.convertStrToDuration(value))
  }

  def getReqEstCountStr(req: TiDAGRequest): String =
    if (req.getEstimatedCount > 0) {
      import java.text.DecimalFormat
      val df = new DecimalFormat("#.#")
      s" EstimatedCount:${df.format(req.getEstimatedCount)}"
    } else ""

  def rowToInternalRow(row: Row,
                       outputTypes: Seq[sql.types.DataType],
                       converters: Seq[Any => Any]): InternalRow = {
    val mutableRow = new GenericInternalRow(outputTypes.length)
    for (i <- outputTypes.indices) {
      mutableRow(i) = converters(i)(row(i))
    }

    mutableRow
  }

  def extractDAGReq(df: DataFrame): TiDAGRequest = {
    val executedPlan = df.queryExecution.executedPlan
    val copRDD = executedPlan.find(e => e.isInstanceOf[ColumnarCoprocessorRDD])
    val regionTaskExec = executedPlan.find(e => e.isInstanceOf[ColumnarRegionTaskExec])
    if (copRDD.isDefined) {
      copRDD.get
        .asInstanceOf[ColumnarCoprocessorRDD]
        .tiRDDs
        .head
        .dagRequest
    } else if (regionTaskExec.isDefined) {
      regionTaskExec.get
        .asInstanceOf[ColumnarRegionTaskExec]
        .dagRequest
    } else {
      throw new UnsupportedOperationException(
        "cannot find ColumnarCoprocessorRDD or " +
          "ColumnarRegionTaskExec in DataFrame."
      )
    }
  }
}
