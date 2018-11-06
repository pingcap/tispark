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

package com.pingcap.tikv.expression;

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.*;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Objects;

public class ArithmeticBinaryExpression implements Expression {
  public enum Type {
    PLUS,
    MINUS,
    MULTIPLY,
    DIVIDE,
    BIT_AND,
    BIT_OR,
    BIT_XOR
  }

  public static ArithmeticBinaryExpression plus(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(PLUS, left, right);
  }

  public static ArithmeticBinaryExpression minus(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(MINUS, left, right);
  }

  public static ArithmeticBinaryExpression multiply(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(MULTIPLY, left, right);
  }

  public static ArithmeticBinaryExpression divide(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(DIVIDE, left, right);
  }

  public static ArithmeticBinaryExpression bitAnd(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(BIT_AND, left, right);
  }

  public static ArithmeticBinaryExpression bitOr(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(BIT_OR, left, right);
  }

  public static ArithmeticBinaryExpression bitXor(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(BIT_XOR, left, right);
  }

  private final Expression left;
  private final Expression right;
  private final Type compType;

  public ArithmeticBinaryExpression(Type type, Expression left, Expression right) {
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.compType = requireNonNull(type, "type is null");
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  public Type getCompType() {
    return compType;
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof ArithmeticBinaryExpression)) {
      return false;
    }

    ArithmeticBinaryExpression that = (ArithmeticBinaryExpression) other;
    return (compType == that.compType)
        && Objects.equals(left, that.left)
        && Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compType, left, right);
  }

  @Override
  public String toString() {
    return String.format("[%s %s %s]", getLeft(), getCompType(), getRight());
  }
}
