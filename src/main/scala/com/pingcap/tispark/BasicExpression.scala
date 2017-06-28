package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.expression.{TiColumnRef, TiConstant, TiExpr}
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, AttributeReference, Divide, Expression, Literal, Multiply, Remainder, Subtract}

object BasicExpression {
  implicit def stringToByteString(str: String): ByteString = ByteString.copyFromUtf8(str)
  type TiPlus = com.pingcap.tikv.expression.scalar.Plus
  type TiMinus = com.pingcap.tikv.expression.scalar.Minus
  type TiMultiply = com.pingcap.tikv.expression.scalar.Multiply
  type TiDivide = com.pingcap.tikv.expression.scalar.Divide
  type TiMod = com.pingcap.tikv.expression.scalar.Mod

  def convertToTiExpr(expr: Expression): Option[TiExpr] = {
    expr match {
      case Literal(value, _) => {
        Some(TiConstant.create(value))
      }

      case Add(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiPlus(lhs, rhs))

      case Subtract(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiMinus(lhs, rhs))

      case Multiply(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiMultiply(lhs, rhs))

      case Divide(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiDivide(lhs, rhs))

      case Remainder(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(new TiMod(lhs, rhs))

      case Alias(BasicExpression(child), _) =>
        Some(child)

      // TODO: Are all AttributeReference column reference in such context?
      case attr: AttributeReference =>
        // Do we need add ValToType in TiExpr?
        // Some(TiExpr.create().setValue(attr.name).toProto)
        Some(TiColumnRef.create(attr.name))

      // TODO: Remove it and let it fail once done all translation
      case _ => Option.empty[TiExpr]
    }
  }

  def unapply(expr: Expression): Option[TiExpr] = convertToTiExpr(expr)
}
