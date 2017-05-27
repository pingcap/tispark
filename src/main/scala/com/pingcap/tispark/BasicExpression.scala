package com.pingcap.tispark

import collection.JavaConverters._
import com.pingcap.tidb.tipb.{Expr, ExprType}
import org.apache.spark.sql.catalyst.expressions.{Abs, Alias, AttributeReference, BinaryArithmetic, Expression, Literal}
import com.google.proto4pingcap.ByteString
import com.pingcap.tikv.TiConfiguration
import com.pingcap.tikv.expression.{TiAggregateFunction, TiConstant, TiExpr, TiScalarExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Average, Count, Sum}

object BasicExpression {
  implicit def stringToByteString(str: String): ByteString = ByteString.copyFromUtf8(str)

  def unapply(expr: Expression): Option[Expr] = {
    expr match {
        // TODO: Translate basic literals
      case _: Literal =>
        Some(TiConstant.create(null).toProto)

      case BinaryArithmetic(BasicExpression(lhs), BasicExpression(rhs)) =>
        // TODO: figure how to set type for BinaryArithmetic
        // TODO: translate opcode itself
        Some(TiScalarExpression.create(null).toProto)

      case Sum(BasicExpression(op)) =>
        Some(TiAggregateFunction.create(TiAggregateFunction.AggFunc.Sum).toProto)

      case Average(BasicExpression(op)) =>
        Some (TiAggregateFunction.create(TiAggregateFunction.AggFunc.Average).toProto)

      case Count(BasicExpression(op)) =>
        Some (TiAggregateFunction.create(TiAggregateFunction.AggFunc.Count).toProto)

      case Alias(BasicExpression(child), _) =>
        Some(child)

      // TODO: Are all AttributeReference column reference in such context?
      case attr: AttributeReference =>
        // Do we need add ValToType in TiExpr?
        // Some(TiExpr.create().setValue(attr.name).toProto)
        Some(Expr.newBuilder()
          .setVal(attr.name)
          .build)

        // TODO: Remove it and let it fail once done all translation
      case _ => Some(Expr.getDefaultInstance)
    }
  }
}
