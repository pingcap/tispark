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


import com.pingcap.tikv.types._
import org.apache.spark.sql
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.types.DataType


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

  def isSupportedBasicExpression(expr: Expression) = {
    !BasicExpression.convertToTiExpr(expr).isEmpty
  }

  def isSupportedProjection(expr: Expression): Boolean = {
    expr.find(child => !isSupportedBasicExpression(child)).isEmpty
  }

  def isSupportedFilter(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // 1. if contains UDF / functions that cannot be folded
  def isSupportedGroupingExpr(expr: Expression): Boolean = {
    isSupportedBasicExpression(expr)
  }

  // convert tikv-java client FieldType to Spark DataType
  def toSparkDataType(tp: TiDataType): DataType = {
    tp match {
      case _: RawBytesType => sql.types.BinaryType
      case _: BytesType => sql.types.StringType
      case _: IntegerType => sql.types.LongType
      case _: DecimalType => sql.types.DoubleType
      case _: TimestampType => sql.types.TimestampType
      case _: DateType => sql.types.DateType
    }
  }

  def fromSparkType(tp: DataType): TiDataType = {
    tp match {
      case _: sql.types.BinaryType => DataTypeFactory.of(Types.TYPE_BLOB)
      case _: sql.types.StringType => DataTypeFactory.of(Types.TYPE_VARCHAR)
      case _: sql.types.LongType => DataTypeFactory.of(Types.TYPE_LONG)
      case _: sql.types.DoubleType => DataTypeFactory.of(Types.TYPE_NEW_DECIMAL)
      case _: sql.types.TimestampType => DataTypeFactory.of(Types.TYPE_TIMESTAMP)
      case _: sql.types.DateType => DataTypeFactory.of(Types.TYPE_DATE)
    }
  }
}
