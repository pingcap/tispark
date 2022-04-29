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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
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
    child.dataType match {
      // We need to convert MySQLType: TypeTiny,TypeShort,TypeInt24,TypeLong,TypeLonglong,TINYINT,TypeYear to DecimalType in order to push down sum
      // The whole data convert flow: all of the above MySQLType => Spark LongType => Spark DecimalType => MySQLType DecimalType
      // here we just convert Spark LongType => Spark DecimalType, we will convert Spark DecimalType => MySQLType DecimalType later
      case LongType => SpecialSum(child, DecimalType.BigIntDecimal, null)
      case _ =>
        throw new IllegalStateException("only LongType will use PromotedSum to replace Sum")
    }
  }

  def unapply(s: SpecialSum): Option[Expression] =
    s match {
      case s if s.initVal == null => Some(s.child)
      case _ => Option.empty[Expression]
    }
}

object SumNotNullable {
  def apply(child: Expression): SpecialSum = {
    // Use LongType because the push down count always return IntegerType.BIGINT
    // IntegerType.BIGINT will convert to SparkType LongType
    SpecialSum(child, LongType, 0)

  }

  def unapply(s: SpecialSum): Option[Expression] =
    s match {
      case s if s.initVal != null => Some(s.child)
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

  /**
   *  The implement is same as the [[org.apache.spark.sql.catalyst.expressions.aggregate.Sum]]
   * @param newChildren
   */
  protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression = {
    assert(newChildren.size == 1, "Incorrect number of children")
    copy(child = newChildren.head)
  }
}
