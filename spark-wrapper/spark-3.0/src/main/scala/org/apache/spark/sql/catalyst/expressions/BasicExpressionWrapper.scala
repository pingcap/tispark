package org.apache.spark.sql.catalyst.expressions

import com.pingcap.tikv.expression.{ColumnRef, ComparisonBinaryExpression, StringRegExpression, _}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.execution.TiConverter
import org.apache.spark.sql.types.DecimalType

object BasicExpressionWrapper {

  import BasicExpression._

  def convertToTiExpr(expr: Expression): Option[TiExpression] =
    expr match {
      case Literal(value, dataType) =>
        Some(
          Constant.create(convertLiteral(value, dataType), TiConverter.fromSparkType(dataType)))

      case Add(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ArithmeticBinaryExpression.plus(lhs, rhs))

      case Subtract(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ArithmeticBinaryExpression.minus(lhs, rhs))

      case e @ Multiply(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ArithmeticBinaryExpression.multiply(TiConverter.fromSparkType(e.dataType), lhs, rhs))

      case d @ Divide(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ArithmeticBinaryExpression.divide(TiConverter.fromSparkType(d.dataType), lhs, rhs))

      case And(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(LogicalBinaryExpression.and(lhs, rhs))

      case Or(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(LogicalBinaryExpression.or(lhs, rhs))

      case Alias(BasicExpression(child), _) =>
        Some(child)

      case IsNull(BasicExpression(child)) =>
        Some(new TiIsNull(child))

      case IsNotNull(BasicExpression(child)) =>
        Some(new TiNot(new TiIsNull(child)))

      case GreaterThan(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ComparisonBinaryExpression.greaterThan(lhs, rhs))

      case GreaterThanOrEqual(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ComparisonBinaryExpression.greaterEqual(lhs, rhs))

      case LessThan(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ComparisonBinaryExpression.lessThan(lhs, rhs))

      case LessThanOrEqual(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ComparisonBinaryExpression.lessEqual(lhs, rhs))

      case EqualTo(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(ComparisonBinaryExpression.equal(lhs, rhs))

      case Not(EqualTo(BasicExpression(lhs), BasicExpression(rhs))) =>
        Some(ComparisonBinaryExpression.notEqual(lhs, rhs))

      case Not(BasicExpression(child)) =>
        Some(new TiNot(child))

      case StartsWith(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(StringRegExpression.startsWith(lhs, rhs))

      case Contains(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(StringRegExpression.contains(lhs, rhs))

      case EndsWith(BasicExpression(lhs), BasicExpression(rhs)) =>
        Some(StringRegExpression.endsWith(lhs, rhs))

      case Like(BasicExpression(lhs), BasicExpression(rhs), escapeChar) =>
        Some(StringRegExpression.like(lhs, rhs, escapeChar))

      // Coprocessor has its own behavior of type promoting and overflow check
      // so we simply remove it from expression and let cop handle it
      case CheckOverflow(BasicExpression(expr), dec: DecimalType, _) =>
        expr.setDataType(TiConverter.fromSparkType(dec))
        Some(expr)

      case PromotePrecision(BasicExpression(expr)) =>
        Some(expr)

      case PromotePrecision(Cast(BasicExpression(expr), dec: DecimalType, _)) =>
        expr.setDataType(TiConverter.fromSparkType(dec))
        Some(expr)

      case PromotePrecision(BasicExpression(expr)) =>
        Some(expr)

      // TODO: Are all AttributeReference column reference in such context?
      case attr: AttributeReference =>
        Some(ColumnRef.create(attr.name, TiConverter.fromSparkType(attr.dataType)))

      case uAttr: UnresolvedAttribute =>
        Some(ColumnRef.create(uAttr.name, TiConverter.fromSparkType(uAttr.dataType)))

      // TODO: Remove it and let it fail once done all translation
      case _ => Option.empty[TiExpression]
    }
}
