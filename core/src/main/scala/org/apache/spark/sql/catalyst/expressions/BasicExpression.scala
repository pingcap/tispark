/*
 *
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
 *
 */

package org.apache.spark.sql.catalyst.expressions

import java.sql.Timestamp

import com.pingcap.tikv.expression._
import com.pingcap.tikv.region.RegionStoreClient.RequestTypes
import com.pingcap.tispark.utils.ReflectionUtil
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.TiConverter
import org.apache.spark.sql.types._
import org.joda.time.DateTime

object BasicExpression {
  type TiDataType = com.pingcap.tikv.types.DataType
  type TiExpression = com.pingcap.tikv.expression.Expression
  type TiNot = com.pingcap.tikv.expression.Not
  type TiIsNull = com.pingcap.tikv.expression.IsNull

  def convertLiteral(value: Any, dataType: DataType): Any =
    // all types from literals are passed according to DataType's InternalType definition
    if (value == null || dataType == null) {
      null
    } else {
      dataType match {
        // In Spark Date is encoded as integer of days after 1970-01-01 UTC
        // and this number of date has compensate of timezone
        // and must be restored by DateTimeUtils.daysToMillis
        case DateType =>
          new DateTime(DateTimeUtils.daysToMillis(value.asInstanceOf[DateTimeUtils.SQLDate]))
        case TimestampType => new Timestamp(value.asInstanceOf[Long] / 1000)
        case StringType => value.toString
        case _: DecimalType => value.asInstanceOf[Decimal].toBigDecimal.bigDecimal
        case _ => value
      }
    }

  def isSupportedExpression(expr: Expression, requestTypes: RequestTypes): Boolean =
    requestTypes match {
      case RequestTypes.REQ_TYPE_DAG if expr.children.nonEmpty =>
        val childType = expr.children.head.dataType
        // if any child's data type is different from others,
        // we do not support this expression in DAG mode
        expr.children.forall(
          (e: Expression) =>
            sameCopType(childType, e.dataType) && isSupportedExpression(
              e,
              requestTypes
            ) // Do it recursively
        )
      // For other request types we assume them supported
      // by default
      case _ => true
    }

  def sameCopType(lhs: DataType, rhs: DataType): Boolean = {
    (lhs, rhs) match {
      case (_: IntegralType, _: IntegralType) => true
      case (_: DecimalType, _: DecimalType) => true
      case (_: FloatType | _: DoubleType, _: FloatType | _: DoubleType) => true
      case _ => lhs.sameType(rhs)
    }
  }

  def convertToTiExpr(expr: Expression): Option[TiExpression] = {
    ReflectionUtil.convertToTiExpr(expr)
  }

  def unapply(expr: Expression): Option[TiExpression] = {
    convertToTiExpr(expr)
  }
}
