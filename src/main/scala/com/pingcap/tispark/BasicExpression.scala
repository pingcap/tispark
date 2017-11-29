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

import java.sql.{Date, Timestamp}
import java.util.Objects

import scala.language.implicitConversions

import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.expression.{TiColumnRef, TiConstant, TiExpr}
import com.pingcap.tikv.types.RequestTypes
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Divide, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, IsNotNull, LessThan, LessThanOrEqual, Literal, Multiply, Not, Remainder, Subtract}
import org.apache.spark.sql.types._

object BasicExpression {
  implicit def stringToByteString(str: String): ByteString = ByteString.copyFromUtf8(str)

  type TiPlus = com.pingcap.tikv.expression.scalar.Plus
  type TiMinus = com.pingcap.tikv.expression.scalar.Minus
  type TiMultiply = com.pingcap.tikv.expression.scalar.Multiply
  type TiDivide = com.pingcap.tikv.expression.scalar.Divide
  type TiIsNull = com.pingcap.tikv.expression.scalar.IsNull
  type TiGreaterEqual = com.pingcap.tikv.expression.scalar.GreaterEqual
  type TiGreaterThan = com.pingcap.tikv.expression.scalar.GreaterThan
  type TiLessEqual = com.pingcap.tikv.expression.scalar.LessEqual
  type TiLessThan = com.pingcap.tikv.expression.scalar.LessThan
  type TiEqual = com.pingcap.tikv.expression.scalar.Equal
  type TiNotEqual = com.pingcap.tikv.expression.scalar.NotEqual
  type TiNot = com.pingcap.tikv.expression.scalar.Not

  final val MILLISEC_PER_DAY: Long = 60 * 60 * 24 * 1000

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
        case DateType       => new Date(MILLISEC_PER_DAY * value.asInstanceOf[Int])
        case TimestampType  => new Timestamp(value.asInstanceOf[Long])
        case StringType     => value.toString
        case _: DecimalType => value.asInstanceOf[Decimal].toBigDecimal.bigDecimal
        case _              => value
      }
    }

  def isSupportedExpression(expr: Expression, requestMode: Int): Boolean = {
    // Currently, only DAG mode needs to infer type
    if (requestMode == RequestTypes.REQ_TYPE_DAG) {
      if (expr.children.nonEmpty) {
        val childType = expr.children.head.dataType

        // if any child's data type is different from others,
        // we do not support this expression to push down in
        // DAG mode
        for (child <- expr.children) {
          if (!childType.equals(child.dataType) ||
              // Do it recursively
              !isSupportedExpression(child, requestMode)) {
            return false
          }
        }
      }
    }
    true
  }

  def convertToTiExpr(expr: Expression): Option[TiExpr] =
    expr match {
      case Literal(value, dataType) =>
        Some(TiConstant.create(convertLiteral(value, dataType)))

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
        Some(TiColumnRef.create(attr.name))

      // TODO: Remove it and let it fail once done all translation
      case _ => Option.empty[TiExpr]
    }

  def unapply(expr: Expression): Option[TiExpr] = convertToTiExpr(expr)
}
