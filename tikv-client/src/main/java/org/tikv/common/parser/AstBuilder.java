/*
 * Copyright 2020 PingCAP, Inc.
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

package org.tikv.common.parser;

import static org.tikv.common.types.IntegerType.BOOLEAN;

import com.google.common.primitives.Doubles;
import com.pingcap.tikv.parser.MySqlParser;
import com.pingcap.tikv.parser.MySqlParserBaseVisitor;
import org.tikv.common.exception.UnsupportedSyntaxException;
import org.tikv.common.expression.ArithmeticBinaryExpression;
import org.tikv.common.expression.ColumnRef;
import org.tikv.common.expression.ComparisonBinaryExpression;
import org.tikv.common.expression.Constant;
import org.tikv.common.expression.Expression;
import org.tikv.common.expression.FuncCallExpr;
import org.tikv.common.expression.FuncCallExpr.Type;
import org.tikv.common.expression.LogicalBinaryExpression;
import org.tikv.common.meta.TiTableInfo;
import com.pingcap.tikv.parser.MySqlParser.ExpressionContext;
import com.pingcap.tikv.parser.MySqlParser.FunctionNameBaseContext;
import org.tikv.common.types.IntegerType;
import org.tikv.common.types.RealType;
import org.antlr.v4.runtime.tree.TerminalNode;

// AstBuilder will convert ParseTree into Ast Node.
// In tikv java client, we only need to parser expression
// which is used by partition pruning.
public class AstBuilder extends MySqlParserBaseVisitor<Expression> {
  private TiTableInfo tableInfo;

  public AstBuilder() {}

  public AstBuilder(TiTableInfo tableInfo) {
    this.tableInfo = tableInfo;
  }

  public Expression visitSimpleId(com.pingcap.tikv.parser.MySqlParser.SimpleIdContext ctx) {
    if (ctx.ID() != null) {
      return createColRef(ctx.ID().getSymbol().getText());
    }

    if (ctx.functionNameBase() != null) {
      return createColRef(ctx.functionNameBase().getText());
    }

    throw new UnsupportedSyntaxException(ctx.getParent().toString() + ": it is not supported");
  }

  private Expression createColRef(String id) {
    if (tableInfo != null) {
      return ColumnRef.create(id, tableInfo);
    } else {
      return ColumnRef.create(id);
    }
  }

  @Override
  public Expression visitUid(com.pingcap.tikv.parser.MySqlParser.UidContext ctx) {
    if (ctx.REVERSE_QUOTE_ID() != null) {
      return createColRef(ctx.REVERSE_QUOTE_ID().getSymbol().getText());
    }
    return visitSimpleId(ctx.simpleId());
  }

  @Override
  public Expression visitScalarFunctionCall(com.pingcap.tikv.parser.MySqlParser.ScalarFunctionCallContext ctx) {
    FunctionNameBaseContext fnNameCtx = ctx.scalarFunctionName().functionNameBase();
    if (fnNameCtx != null) {
      if (fnNameCtx.YEAR() != null) {
        Expression args = visitFunctionArgs(ctx.functionArgs());
        return new FuncCallExpr(args, Type.YEAR);
      }
    }
    return visitChildren(ctx);
  }

  @Override
  public Expression visitFullColumnName(com.pingcap.tikv.parser.MySqlParser.FullColumnNameContext ctx) {
    return visitUid(ctx.uid());
  }

  private Expression parseIntOrLongOrDec(String val) {
    try {
      return Constant.create(Integer.parseInt(val), IntegerType.INT);
    } catch (Exception e) {
      try {
        return Constant.create(Long.parseLong(val), IntegerType.BIGINT);
      } catch (Exception e2) {
        return Constant.create(Double.parseDouble(val), RealType.DOUBLE);
      }
    }
  }

  @Override
  public Expression visitDecimalLiteral(com.pingcap.tikv.parser.MySqlParser.DecimalLiteralContext ctx) {
    if (ctx.ONE_DECIMAL() != null) {
      String val = ctx.ONE_DECIMAL().getSymbol().getText();
      return parseIntOrLongOrDec(val);
    }
    if (ctx.TWO_DECIMAL() != null) {
      String val = ctx.TWO_DECIMAL().getSymbol().getText();
      return parseIntOrLongOrDec(val);
    }
    if (ctx.DECIMAL_LITERAL() != null) {
      String val = ctx.DECIMAL_LITERAL().getSymbol().getText();
      return parseIntOrLongOrDec(val);
    }

    if (ctx.ZERO_DECIMAL() != null) {
      String val = ctx.ZERO_DECIMAL().getSymbol().getText();
      return parseIntOrLongOrDec(val);
    }

    throw new UnsupportedSyntaxException(ctx.toString() + ": it is not supported.");
  }

  @Override
  public Expression visitBooleanLiteral(com.pingcap.tikv.parser.MySqlParser.BooleanLiteralContext ctx) {
    if (ctx.FALSE() != null) return Constant.create(0);
    return Constant.create(1, BOOLEAN);
  }

  @Override
  public Expression visitStringLiteral(com.pingcap.tikv.parser.MySqlParser.StringLiteralContext ctx) {
    if (ctx.STRING_LITERAL() != null) {
      StringBuilder sb = new StringBuilder();
      for (TerminalNode str : ctx.STRING_LITERAL()) {
        sb.append(str.getSymbol().getText());
      }
      return Constant.create(sb.toString().replace("\"", ""));
    }
    throw new UnsupportedSyntaxException(ctx.toString() + " is not supported yet");
  }

  @Override
  public Expression visitConstant(com.pingcap.tikv.parser.MySqlParser.ConstantContext ctx) {
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
  public Expression visitConstantExpressionAtom(com.pingcap.tikv.parser.MySqlParser.ConstantExpressionAtomContext ctx) {
    return visitChildren(ctx);
  }

  @Override
  public Expression visitBinaryComparisonPredicate(
      com.pingcap.tikv.parser.MySqlParser.BinaryComparisonPredicateContext ctx) {
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

    throw new UnsupportedSyntaxException(
        ctx.toString() + ": it is not possible reach to this line of code");
  }

  public Expression visitLogicalExpression(com.pingcap.tikv.parser.MySqlParser.LogicalExpressionContext ctx) {
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

    throw new UnsupportedSyntaxException(
        ctx.toString() + ": it is not possible reach to this line of code");
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
    throw new UnsupportedSyntaxException(ctx.toString() + ": it is not supported right now");
  }
}