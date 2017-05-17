package com.pingcap.tispark

import com.pingcap.tidb.tipb.Expr
import org.apache.spark.sql.catalyst.expressions.{Abs, Alias, AttributeReference, BinaryArithmetic, Expression, Literal}

import collection.JavaConverters._
import com.google.proto4pingcap.ByteString

object BasicExpression {
  implicit def stringToByteString(str: String) = ByteString.copyFromUtf8(str)

  def unapply(expr: Expression): Option[Expr] = {
    expr match {
        // TODO: Translate basic literals
      case _: Literal =>
        Some(Expr.newBuilder()
          .setVal("")
          .build)

      case BinaryArithmetic(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(Expr.newBuilder()
          // TODO: translate opcode itself
          .addAllChildren(List(lhs, rhs).asJava)
          .build)

      case Abs(BasicExpression(op)) =>
        Some(Expr.newBuilder()
          .setVal("abs")
          .addChildren(op)
          .build)

      case Alias(BasicExpression(child), _) =>
        Some(child)

      case attr: AttributeReference =>
        // TODO: Are all AttributeReference column reference in such context?
        Some(Expr.newBuilder()
          .setVal(attr.name)
          .build)

        // TODO: Remove it and let it fail once done all translation
      case _ => Some(Expr.getDefaultInstance())
    }
  }
}
