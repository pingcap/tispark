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

package org.apache.spark.sql.catalyst.expressions.aggregate

import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{
  Add,
  AttributeReference,
  Cast,
  Coalesce,
  Expression,
  ExpressionDescription,
  Literal
}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._

object PromotedSum {
  def apply(child: Expression): SpecialSum = {
    val retType = child.dataType match {
      case DecimalType.Fixed(precision, scale) =>
        DecimalType.bounded(precision + 10, scale)
      case _ => DoubleType
    }

    SpecialSum(child, retType, null)
  }

  def unapply(s: SpecialSum): Option[Expression] =
    s match {
      case s.initVal if s.initVal == null => Some(s.child)
      case _ => Option.empty[Expression]
    }
}

object SumNotNullable {
  def apply(child: Expression): SpecialSum = {
    val retType = child.dataType match {
      case DecimalType.Fixed(precision, scale) =>
        DecimalType.bounded(precision + 10, scale)
      case _: IntegralType => LongType
      case _ => DoubleType
    }

    SpecialSum(child, retType, 0)
  }

  def unapply(s: SpecialSum): Option[Expression] =
    s match {
      case s.initVal if s.initVal == null => Some(s.child)
      case _ => Option.empty[Expression]
    }
}

@ExpressionDescription(
  usage =
    "_FUNC_(expr) - Returns the sum calculated from values of a group. Result type is promoted to double/decimal.")
case class SpecialSum(child: Expression, retType: DataType, initVal: Any)
    extends DeclarativeAggregate {

  override lazy val aggBufferAttributes: Seq[AttributeReference] = sum :: Nil
  override lazy val initialValues: Seq[Expression] = {
    val longVal = initVal match {
      case i: Integer => i.toLong
      case other => other
    }
    Seq( /* sum = */ Literal.create(longVal, sumDataType))
  }
  override lazy val updateExpressions: Seq[Expression] = {
    if (child.nullable) {
      Seq(
        /* sum = */
        Coalesce(Seq(Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)), sum)))
    } else {
      Seq(
        /* sum = */
        Add(Coalesce(Seq(sum, zero)), Cast(child, sumDataType)))
    }
  }
  override lazy val mergeExpressions: Seq[Expression] = {
    Seq(
      /* sum = */
      Coalesce(Seq(Add(Coalesce(Seq(sum.left, zero)), sum.right), sum.left)))
  }
  override lazy val evaluateExpression: Expression = sum
  private lazy val resultType = retType
  private lazy val sumDataType = resultType
  private lazy val sum = AttributeReference("rewriteSum", sumDataType)()
  private lazy val zero = Cast(Literal(0), sumDataType)

  override def children: Seq[Expression] = child :: Nil

  override def nullable: Boolean = true

  // Return data type.
  override def dataType: DataType = resultType

  override def checkInputDataTypes(): TypeCheckResult =
    TypeUtils.checkForNumericExpr(child.dataType, "function sum")
}
