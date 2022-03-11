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

import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.BIT_AND;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.BIT_OR;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.BIT_XOR;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.DIVIDE;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.MINUS;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.MULTIPLY;
import static com.pingcap.tikv.expression.ArithmeticBinaryExpression.Type.PLUS;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.types.DataType;
import java.util.List;
import java.util.Objects;

public class ArithmeticBinaryExpression extends Expression {
  private final Expression left;
  private final Expression right;
  private final Type compType;

  public ArithmeticBinaryExpression(
      DataType dataType, Type type, Expression left, Expression right) {
    super(dataType);
    resolved = true;
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.compType = requireNonNull(type, "type is null");
  }

  public static ArithmeticBinaryExpression plus(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, PLUS, left, right);
  }

  public static ArithmeticBinaryExpression minus(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, MINUS, left, right);
  }

  public static ArithmeticBinaryExpression multiply(
      DataType dataType, Expression left, Expression right) {
    return new ArithmeticBinaryExpression(dataType, MULTIPLY, left, right);
  }

  public static ArithmeticBinaryExpression multiply(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, MULTIPLY, left, right);
  }

  public static ArithmeticBinaryExpression divide(
      DataType dataType, Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, DIVIDE, left, right);
  }

  public static ArithmeticBinaryExpression divide(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, DIVIDE, left, right);
  }

  public static ArithmeticBinaryExpression bitAnd(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, BIT_AND, left, right);
  }

  public static ArithmeticBinaryExpression bitOr(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, BIT_OR, left, right);
  }

  public static ArithmeticBinaryExpression bitXor(Expression left, Expression right) {
    return new ArithmeticBinaryExpression(left.dataType, BIT_XOR, left, right);
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

  public enum Type {
    PLUS,
    MINUS,
    MULTIPLY,
    DIVIDE,
    BIT_AND,
    BIT_OR,
    BIT_XOR
  }
}
