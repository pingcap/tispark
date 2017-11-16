/*
 * Copyright 2017 PingCAP, Inc.
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

package com.pingcap.tispark


import java.util
import java.util.concurrent.TimeUnit
import java.util.stream.{Collector, Collectors}
import scala.collection.JavaConversions._

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tikv.kvproto.Kvrpcpb.{CommandPri, IsolationLevel}
import com.pingcap.tikv.meta.{TiColumnInfo, TiTableInfo}
import com.pingcap.tikv.types._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, NamedExpression}
import org.apache.spark.sql.types.{DataType, DataTypes, MetadataBuilder, StructField, StructType}
import org.apache.spark.{SparkConf, sql}

import scala.collection.mutable


object TiUtils {
  type TiSum = com.pingcap.tikv.expression.aggregate.Sum
  type TiCount = com.pingcap.tikv.expression.aggregate.Count
  type TiMin = com.pingcap.tikv.expression.aggregate.Min
  type TiMax = com.pingcap.tikv.expression.aggregate.Max
  type TiFirst = com.pingcap.tikv.expression.aggregate.First
  type TiDataType = com.pingcap.tikv.types.DataType
  type TiTypes = com.pingcap.tikv.types.Types

  def isSupportedAggregate(aggExpr: AggregateExpression): Boolean = {
    aggExpr.aggregateFunction match {
      case Average(_) | Sum(_) | Count(_) | Min(_) | Max(_) =>
        !aggExpr.isDistinct &&
          !aggExpr.aggregateFunction
            .children.exists(expr => !isSupportedBasicExpression(expr))
      case _ => false
    }
  }

  def isSupportedBasicExpression(expr: Expression): Boolean = {
    BasicExpression.convertToTiExpr(expr).fold(false) {
      _.isSupportedExpr
    }
  }

  /**
    * Is expression allowed to be pushed down
    *
    * @param expr the expression to examine
    * @return whether expression can be pushed down
    */
  def isPushDownSupported(expr: Expression, source: TiDBRelation): Boolean = {
    val nameTypeMap = mutable.HashMap[String, com.pingcap.tikv.types.DataType]()
    source.table.getColumns.foreach((info: TiColumnInfo) => nameTypeMap(info.getName) = info.getType)

    if (expr.children.isEmpty) {
      expr match {
        // bit type is not allowed to be pushed down
        case attr: AttributeReference =>
          return nameTypeMap.contains(attr.name) &&
            !nameTypeMap.get(attr.name).head.isInstanceOf[BitType]
        case _ => return true
      }
    } else {
      for (expr <- expr.children) {
        if (!isPushDownSupported(expr, source)) {
          return false
        }
      }
    }

    true
  }

  def isSupportedFilter(expr: Expression, source: TiDBRelation): Boolean = {
    isSupportedBasicExpression(expr) && isPushDownSupported(expr, source)
  }

  // if contains UDF / functions that cannot be folded
  def isSupportedGroupingExpr(expr: NamedExpression): Boolean = isSupportedBasicExpression(expr)

  // convert tikv-java client FieldType to Spark DataType
  def toSparkDataType(tp: TiDataType): DataType = {
    tp match {
      case _: RawBytesType => sql.types.BinaryType
      case _: BytesType => sql.types.StringType
      case _: IntegerType => sql.types.LongType
      case _: RealType => sql.types.DoubleType
      // we need to make sure that tp.getLength does not result in negative number when casting.
      case _: DecimalType =>
        DataTypes.createDecimalType(
          Math.min(Integer.MAX_VALUE, tp.getLength).asInstanceOf[Int],
          tp.getDecimal)
      case _: TimestampType => sql.types.TimestampType
      case _: DateType => sql.types.DateType
    }
  }

  def fromSparkType(tp: DataType): TiDataType = {
    tp match {
      case _: sql.types.BinaryType => DataTypeFactory.of(Types.TYPE_BLOB)
      case _: sql.types.StringType => DataTypeFactory.of(Types.TYPE_VARCHAR)
      case _: sql.types.LongType => DataTypeFactory.of(Types.TYPE_LONG)
      case _: sql.types.DoubleType => DataTypeFactory.of(Types.TYPE_DOUBLE)
      case _: sql.types.DecimalType => DataTypeFactory.of(Types.TYPE_NEW_DECIMAL)
      case _: sql.types.TimestampType => DataTypeFactory.of(Types.TYPE_TIMESTAMP)
      case _: sql.types.DateType => DataTypeFactory.of(Types.TYPE_DATE)
    }
  }

  def getSchemaFromTable(table: TiTableInfo): StructType = {
    val fields = new Array[StructField](table.getColumns.size())
    for (i <- 0 until table.getColumns.size()) {
      val col = table.getColumns.get(i)
      val metadata = new MetadataBuilder()
        .putString("name", col.getName)
        .build()
      fields(i) = StructField(col.getName, TiUtils.toSparkDataType(col.getType), nullable = true, metadata)
    }
    new StructType(fields)
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

    if (conf.contains(TiConfigConst.META_RELOAD_PERIOD)) {
      tiConf.setMetaReloadPeriod(conf.get(TiConfigConst.META_RELOAD_PERIOD).toInt)
      tiConf.setMetaReloadPeriodUnit(TimeUnit.SECONDS)
    }

    if (conf.contains(TiConfigConst.GRPC_RETRY_TIMES)) {
      tiConf.setRpcRetryTimes(conf.get(TiConfigConst.GRPC_RETRY_TIMES).toInt)
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
      val isolationLevel = IsolationLevel.valueOf(conf.get(TiConfigConst.REQUEST_ISOLATION_LEVEL))
      tiConf.setIsolationLevel(isolationLevel)
    }

    if (conf.contains(TiConfigConst.REQUEST_COMMAND_PRIORITY)) {
      val priority = CommandPri.valueOf(conf.get(TiConfigConst.REQUEST_COMMAND_PRIORITY))
      tiConf.setCommandPriority(priority)
    }

    tiConf
  }
}
