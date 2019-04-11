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

import java.util.concurrent.TimeUnit
import java.util.logging.Logger

import com.pingcap.tikv.expression.ExpressionBlacklist
import com.pingcap.tikv.expression.visitor.{MetaResolver, SupportedExpressionValidator}
import org.tikv.kvproto.Kvrpcpb.{CommandPri, IsolationLevel}
import com.pingcap.tikv.meta.{TiColumnInfo, TiDAGRequest, TiTableInfo}
import com.pingcap.tikv.region.RegionStoreClient.RequestTypes
import com.pingcap.tikv.types._
import com.pingcap.tikv.{types, TiConfiguration}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.aggregate.SortAggregateExec
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.types.{DataType, DataTypes, MetadataBuilder, StructField, StructType}
import org.apache.spark.{sql, SparkConf}

import scala.collection.JavaConversions._
import scala.collection.mutable

object TiUtils {
  type TiDataType = com.pingcap.tikv.types.DataType
  type TiExpression = com.pingcap.tikv.expression.Expression

  /**
   * Literal to be used with the Spark DataFrame's .format method
   */
  val TIDB_SOURCE_NAME = "com.pingcap.tispark"

  private final val logger = Logger.getLogger(getClass.getName)
  private final val MAX_PRECISION = sql.types.DecimalType.MAX_PRECISION

  def isSupportedAggregate(aggExpr: AggregateExpression,
                           tiDBRelation: TiDBRelation,
                           blacklist: ExpressionBlacklist): Boolean =
    aggExpr.aggregateFunction match {
      case Average(_) | Sum(_) | SumNotNullable(_) | PromotedSum(_) | Count(_) | Min(_) | Max(_) =>
        !aggExpr.isDistinct &&
          aggExpr.aggregateFunction.children
            .forall(isSupportedBasicExpression(_, tiDBRelation, blacklist))
      case _ => false
    }

  def isSupportedBasicExpression(expr: Expression,
                                 tiDBRelation: TiDBRelation,
                                 blacklist: ExpressionBlacklist): Boolean = {
    if (!BasicExpression.isSupportedExpression(expr, RequestTypes.REQ_TYPE_DAG)) return false

    BasicExpression.convertToTiExpr(expr).fold(false) { expr: TiExpression =>
      MetaResolver.resolve(expr, tiDBRelation.table)
      return SupportedExpressionValidator.isSupportedExpression(expr, blacklist)
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
    source.table.getColumns
      .foreach((info: TiColumnInfo) => nameTypeMap(info.getName) = info.getType)

    if (expr.children.isEmpty) {
      expr match {
        // bit/duration type is not allowed to be pushed down
        case attr: AttributeReference if nameTypeMap.contains(attr.name) =>
          val head = nameTypeMap.get(attr.name).head
          return !head.isInstanceOf[BitType]
        // TODO:Currently we do not support literal null type push down
        // when Constant is ready to support literal null or we have other
        // options, remove this.
        case constant: Literal =>
          return constant.value != null
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

  def isSupportedFilter(expr: Expression,
                        source: TiDBRelation,
                        blacklist: ExpressionBlacklist): Boolean =
    isSupportedBasicExpression(expr, source, blacklist) && isPushDownSupported(expr, source)

  // if contains UDF / functions that cannot be folded
  def isSupportedGroupingExpr(expr: NamedExpression,
                              source: TiDBRelation,
                              blacklist: ExpressionBlacklist): Boolean =
    isSupportedBasicExpression(expr, source, blacklist) && isPushDownSupported(expr, source)

  // convert tikv-java client FieldType to Spark DataType
  def toSparkDataType(tp: TiDataType): DataType =
    tp match {
      case _: StringType => sql.types.StringType
      case _: BytesType  => sql.types.BinaryType
      case _: IntegerType =>
        if (tp.asInstanceOf[IntegerType].isUnsignedLong) {
          DataTypes.createDecimalType(20, 0)
        } else {
          sql.types.LongType
        }
      case _: RealType => sql.types.DoubleType
      // we need to make sure that tp.getLength does not result in negative number when casting.
      // Decimal precision cannot exceed MAX_PRECISION.
      case _: DecimalType =>
        var len = tp.getLength
        if (len > MAX_PRECISION) {
          logger.warning(
            "Decimal precision exceeding MAX_PRECISION=" + MAX_PRECISION + ", value will be truncated"
          )
          len = MAX_PRECISION
        }
        DataTypes.createDecimalType(
          len.asInstanceOf[Int],
          tp.getDecimal
        )
      case _: DateTimeType  => sql.types.TimestampType
      case _: TimestampType => sql.types.TimestampType
      case _: DateType      => sql.types.DateType
      case _: EnumType      => sql.types.StringType
      case _: SetType       => sql.types.StringType
      case _: JsonType      => sql.types.StringType
      case _: TimeType      => sql.types.LongType
    }

  def fromSparkType(tp: DataType): TiDataType =
    tp match {
      case _: sql.types.BinaryType    => BytesType.BLOB
      case _: sql.types.StringType    => StringType.VARCHAR
      case _: sql.types.LongType      => IntegerType.BIGINT
      case _: sql.types.DoubleType    => RealType.DOUBLE
      case _: sql.types.DecimalType   => DecimalType.DECIMAL
      case _: sql.types.TimestampType => TimestampType.TIMESTAMP
      case _: sql.types.DateType      => DateType.DATE
    }

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
        TiUtils.toSparkDataType(col.getType),
        nullable = !notNull,
        metadata
      )
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
    tiConf
  }

  def getReqEstCountStr(req: TiDAGRequest): String =
    if (req.getEstimatedCount > 0) {
      import java.text.DecimalFormat
      val df = new DecimalFormat("#.#")
      s" EstimatedCount:${df.format(req.getEstimatedCount)}"
    } else ""

  /**
   * Migrant from Spark 2.1.1 to support non-partial aggregate
   */
  def planAggregateWithoutPartial(groupingExpressions: Seq[NamedExpression],
                                  aggregateExpressions: Seq[AggregateExpression],
                                  resultExpressions: Seq[NamedExpression],
                                  child: SparkPlan): SparkPlan = {
    val completeAggregateExpressions = aggregateExpressions.map(_.copy(mode = Complete))
    val completeAggregateAttributes = completeAggregateExpressions.map(_.resultAttribute)
    SortAggregateExec(
      requiredChildDistributionExpressions = Some(groupingExpressions),
      groupingExpressions = groupingExpressions,
      aggregateExpressions = completeAggregateExpressions,
      aggregateAttributes = completeAggregateAttributes,
      initialInputBufferOffset = 0,
      resultExpressions = resultExpressions,
      child = child
    )
  }
}
