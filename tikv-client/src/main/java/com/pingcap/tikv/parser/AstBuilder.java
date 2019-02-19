package com.pingcap.tikv.parser;

import com.google.common.primitives.Doubles;
import com.pingcap.tikv.exception.UnsupportedSyntaxException;
import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.parser.MySqlParser.ExpressionContext;
import com.pingcap.tikv.types.RealType;

public class AstBuilder extends MySqlParserBaseVisitor<Expression> {
  public Expression visitSimpleId(MySqlParser.SimpleIdContext ctx) {
    if (ctx.ID() != null) {
      return ColumnRef.create(ctx.ID().getSymbol().getText());
    }

    if (ctx.engineName() != null) {
      return Constant.create(ctx.engineName().getText());
    }

    throw new UnsupportedSyntaxException(ctx.getParent().toString() + "is not supported");
  }

  @Override
  public Expression visitUid(MySqlParser.UidContext ctx) {
    return visitSimpleId(ctx.simpleId());
  }

  @Override
  public Expression visitFullColumnName(MySqlParser.FullColumnNameContext ctx) {
    return visitUid(ctx.uid());
  }

  @Override
  public Expression visitDecimalLiteral(MySqlParser.DecimalLiteralContext ctx) {
    if (ctx.ONE_DECIMAL() != null) {
      return Constant.create(
          Doubles.tryParse(ctx.ONE_DECIMAL().getSymbol().getText()), RealType.DOUBLE);
    }
    if (ctx.TWO_DECIMAL() != null) {
      return Constant.create(
          Doubles.tryParse(ctx.TWO_DECIMAL().getSymbol().getText()), RealType.DOUBLE);
    }
    if (ctx.DECIMAL_LITERAL() != null) {
      return Constant.create(
          Doubles.tryParse(ctx.DECIMAL_LITERAL().getSymbol().getText()), RealType.DOUBLE);
    }

    throw new UnsupportedSyntaxException(ctx.toString() + "is not supported.");
  }

  @Override
  public Expression visitBooleanLiteral(MySqlParser.BooleanLiteralContext ctx) {
    throw new UnsupportedSyntaxException("boolean type is not supported yet");
  }

  @Override
  public Expression visitStringLiteral(MySqlParser.StringLiteralContext ctx) {
    if (ctx.STRING_LITERAL() != null) {
      return Constant.create(ctx.STRING_LITERAL().get(0).getSymbol().getText());
    }
    throw new UnsupportedSyntaxException(ctx.toString() + " is not supported yet");
  }

  @Override
  public Expression visitConstant(MySqlParser.ConstantContext ctx) {
    if (ctx.nullLiteral != null) {
      return Constant.create(null);
    }

    if (ctx.booleanLiteral() != null) {
      return visitBooleanLiteral(ctx.booleanLiteral());
    }

    if (ctx.decimalLiteral() != null) {
      return visitDecimalLiteral(ctx.decimalLiteral());
    }

    if (ctx.stringLiteral() != null) {
      return visitStringLiteral(ctx.stringLiteral());
    }

    if (ctx.REAL_LITERAL() != null) {
      return Constant.create(
          Doubles.tryParse(ctx.REAL_LITERAL().getSymbol().getText()), RealType.REAL);
    }

    throw new UnsupportedSyntaxException(ctx.toString() + "not supported constant");
  }

  @Override
  public Expression visitConstantExpressionAtom(MySqlParser.ConstantExpressionAtomContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expression visitBinaryComparisonPredicate(
      MySqlParser.BinaryComparisonPredicateContext ctx) {
    Expression left = visitChildren(ctx.left);
    Expression right = visitChildren(ctx.right);
    switch (ctx.comparisonOperator().getText()) {
      case "<":
        return ComparisonBinaryExpression.lessThan(left, right);
      case "<=":
        return ComparisonBinaryExpression.lessEqual(left, right);
      case "=":
        return ComparisonBinaryExpression.equal(left, right);
      case ">":
        return ComparisonBinaryExpression.greaterThan(left, right);
      case ">=":
        return ComparisonBinaryExpression.greaterEqual(left, right);
    }
    throw new UnsupportedSyntaxException("it is not possible reach this line of code");
  }

  public Expression visitLogicalExpression(MySqlParser.LogicalExpressionContext ctx) {
    ExpressionContext left = ctx.expression(0);
    ExpressionContext right = ctx.expression(1);
    switch (ctx.logicalOperator().getText()) {
      case "and":
        return LogicalBinaryExpression.and(visitChildren(left), visitChildren(right));
      case "or":
        return LogicalBinaryExpression.or(visitChildren(left), visitChildren(right));
      case "xor":
        return LogicalBinaryExpression.xor(visitChildren(left), visitChildren(right));
    }

    throw new UnsupportedSyntaxException("it is not possible reach this line of code");
  }

  @Override
  public Expression visitExpressions(MySqlParser.ExpressionsContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expression visitMathExpressionAtom(MySqlParser.MathExpressionAtomContext ctx) {
    Expression left = visitChildren(ctx.left);
    Expression right = visitChildren(ctx.right);
    switch (ctx.mathOperator().getText()) {
      case "+":
        return ArithmeticBinaryExpression.plus(left, right);
      case "-":
        return ArithmeticBinaryExpression.minus(left, right);
      case "*":
        return ArithmeticBinaryExpression.multiply(left, right);
      case "/":
      case "div":
        return ArithmeticBinaryExpression.divide(left, right);
    }
    throw new UnsupportedSyntaxException("not supported");
  }
}
