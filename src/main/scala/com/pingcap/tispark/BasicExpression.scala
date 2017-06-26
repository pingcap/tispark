package com.pingcap.tispark

import com.google.proto4pingcap.ByteString
import com.pingcap.tidb.tipb.Expr
import com.pingcap.tikv.expression.{TiConstant, TiExpr}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, Sum}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryArithmetic, Expression, Literal}

object BasicExpression {
  implicit def stringToByteString(str: String): ByteString = ByteString.copyFromUtf8(str)

  def unapply(expr: Expression): Option[TiExpr] = {
    expr match {
        // TODO: Translate basic literals
      case _: Literal =>
        Some(TiConstant.create(null))

      case BinaryArithmetic(BasicExpression(lhs), BasicExpression(rhs)) =>
        // TODO: figure how to set type for BinaryArithmetic
        // TODO: translate opcode itself
        Some(null)

      case Sum(BasicExpression(op)) =>
        Some(null)

      case Average(BasicExpression(op)) =>
        Some(null)

      case Count(BasicExpression(op)) =>
        Some(null)

      case Alias(BasicExpression(child), _) =>
        Some(child)

      // TODO: Are all AttributeReference column reference in such context?
      case attr: AttributeReference =>
        // Do we need add ValToType in TiExpr?
        // Some(TiExpr.create().setValue(attr.name).toProto)
        Some(null)

        // TODO: Remove it and let it fail once done all translation
      case _ => Some(null)
    }
  }
}
