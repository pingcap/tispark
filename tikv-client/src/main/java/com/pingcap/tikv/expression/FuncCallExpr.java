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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.expression;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public class FuncCallExpr extends Expression {
  private final Expression child;
  private final Type funcTp;

  public FuncCallExpr(Expression expr, Type funcTp) {
    this.child = expr;
    this.funcTp = funcTp;
  }

  public static FuncCallExpr year(Expression expr) {
    return new FuncCallExpr(expr, Type.YEAR);
  }

  public Type getFuncTp() {
    return this.funcTp;
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(child);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public Expression getExpression() {
    return child;
  }

  private String getFuncString() {
    if (funcTp == Type.YEAR) {
      return "year";
    }
    return "";
  }

  @Override
  public String toString() {
    return String.format("%s(%s)", getFuncString(), getExpression());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof FuncCallExpr)) {
      return false;
    }

    FuncCallExpr that = (FuncCallExpr) other;
    return Objects.equals(child, that.child);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(child);
  }

  // try to evaluate a {@code Constant} literal if its type is
  // varchar or datetime. If such literal cannot be evaluated, return
  // input literal.
  public Constant eval(Constant literal) {
    Function<Constant, Constant> evalFn = FuncCallExprEval.getEvalFn(funcTp);
    if (evalFn != null) return evalFn.apply(literal);
    return literal;
  }

  public enum Type {
    YEAR
  }
}
