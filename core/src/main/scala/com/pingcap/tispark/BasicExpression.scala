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

import java.sql.Timestamp

import com.pingcap.tikv.region.RegionStoreClient.RequestTypes
import org.joda.time.DateTime

import com.pingcap.tikv.expression.{ColumnRef, Constant, Expression}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Subtract}
import org.apache.spark.sql.types._

import scala.language.implicitConversions

object BasicExpression {
  private final val MILLISEC_PER_DAY: Long = 60 * 60 * 24 * 1000

  type TiExpression = com.pingcap.tikv.expression.Expression

  def convertLiteral(value: Any, dataType: DataType): Any =
    // all types from literals are passed according to DataType's InternalType definition
    if (value == null || dataType == null) {
      null
    } else {
      dataType match {
        // In Spark Date is encoded as integer of days after 1970-01-01
        // and sql.Date is constructed as milliseconds after 1970-01-01
        // It seems Date in TiKV coprocessor is encoded as String yyyy-mm-dd,
        // but seems change in DAG mode
        case DateType       => new DateTime(MILLISEC_PER_DAY * value.asInstanceOf[Int])
        case TimestampType  => new Timestamp(value.asInstanceOf[Long] / 1000)
        case StringType     => value.toString
        case _: DecimalType => value.asInstanceOf[Decimal].toBigDecimal.bigDecimal
        case _              => value
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
            childType.eq(e.dataType) && isSupportedExpression(e, requestTypes) // Do it recursively
        )
      // For other request types we assume them supported
      // by default
      case _ => true
    }

  def convertToTiExpr(expr: Expression): Option[TiExpression] =
    expr match {
      case Literal(value, dataType) =>
        Some(Constant.create(convertLiteral(value, dataType)))

      case Add(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiPlus(lhs, rhs))

      case Subtract(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiMinus(lhs, rhs))

      case Multiply(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiMultiply(lhs, rhs))

      case Divide(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiDivide(lhs, rhs))

      case Alias(BasicExpression(child), _) =>
        Some(child)

      case IsNotNull(BasicExpression(child)) =>
        Some(new TiNot(new TiIsNull(child)))

      case GreaterThan(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiGreaterThan(lhs, rhs))

      case GreaterThanOrEqual(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiGreaterEqual(lhs, rhs))

      case LessThan(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiLessThan(lhs, rhs))

      case LessThanOrEqual(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiLessEqual(lhs, rhs))

      case EqualTo(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiEqual(lhs, rhs))

      case Not(EqualTo(BasicExpression(lhs), BasicExpression(rhs))) =>
        Some(new TiNotEqual(lhs, rhs))

      case Not(BasicExpression(child)) =>
        Some(new TiNot(child))

      // TODO: Are all AttributeReference column reference in such context?
      case attr: AttributeReference =>
        // Do we need add ValToType in TiExpr?
        // Some(TiExpr.create().setValue(attr.name).toProto)
        Some(ColumnRef.create(attr.name))

      // TODO: Remove it and let it fail once done all translation
      case _ => Option.empty[Expression]
    }

  def unapply(expr: Expression): Option[Expression] = convertToTiExpr(expr)
}
