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


import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.types.BytesType;

import java.util.List;
import java.util.Objects;

import static com.pingcap.tikv.expression.StringRegExpression.Type.*;
import static java.util.Objects.requireNonNull;

public class StringRegExpression implements Expression {
  public enum Type {
    STARTS_WITH,
    CONTAINS,
    ENDS_WITH
  }

  public static StringRegExpression startsWith(Expression left, Expression right) {
    if (right instanceof Constant && ((Constant) right).getType() instanceof BytesType) {
      right = Constant.create(((Constant) right).getValue() + "%", ((Constant) right).getType());
    } else {
      System.out.println("Impossible");
    }
    return new StringRegExpression(STARTS_WITH, left, right);
  }

  public static StringRegExpression contains(Expression left, Expression right) {
    return new StringRegExpression(CONTAINS, left, right);
  }

  public static StringRegExpression endsWith(Expression left, Expression right) {
    return new StringRegExpression(ENDS_WITH, left, right);
  }

  private final Expression left;
  private final Expression right;
  private final Type regType;

  public StringRegExpression(Type type, Expression left, Expression right) {
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.regType = requireNonNull(type, "type is null");
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  public Expression getLeft() {
    return left;
  }

  public Expression getRight() {
    return right;
  }

  public Type getRegType() {
    return regType;
  }

  @Override
  public String toString() {
    return String.format("[%s %s %s]", getLeft(), getRegType(), getRight());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof StringRegExpression)) {
      return false;
    }

    StringRegExpression that = (StringRegExpression) other;
    return (regType == that.regType) &&
        Objects.equals(left, that.left) &&
        Objects.equals(right, that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regType, left, right);
  }
}
