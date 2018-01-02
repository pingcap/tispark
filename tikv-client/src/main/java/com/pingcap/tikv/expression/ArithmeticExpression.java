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


import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.types.DataType;
import java.util.List;
import java.util.Objects;

public class ArithmeticExpression extends ScalarExpression {

  @Override
  public List<TiExpr> getChildren() {
    return ImmutableList.of(left, right);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
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

  private TiExpr left;
  private TiExpr right;
  private Type compType;

  public ArithmeticExpression(Type type, TiExpr left, TiExpr right) {
    this.left = requireNonNull(left, "left expression is null");
    this.right = requireNonNull(right, "right expression is null");
    this.compType = requireNonNull(type, "type is null");
  }

  public TiExpr getLeft() {
    return left;
  }

  public TiExpr getRight() {
    return right;
  }

  public Type getCompType() {
    return compType;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ArithmeticExpression that = (ArithmeticExpression) other;
    return (compType == that.compType) &&
        left.equals(that.left) &&
        right.equals(that.right);
  }

  @Override
  public int hashCode() {
    return Objects.hash(compType, left, right);
  }
}
