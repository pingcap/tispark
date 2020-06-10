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
import com.pingcap.tikv.types.IntegerType;
import java.util.List;
import java.util.Objects;

public class Not extends Expression {

  private final Expression expression;

  public Not(Expression expression) {
    super(IntegerType.BOOLEAN);
    resolved = true;
    this.expression = requireNonNull(expression, "expression is null");
  }

  public static Not not(Expression expression) {
    return new Not(expression);
  }

  public Expression getExpression() {
    return expression;
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(expression);
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }

  @Override
  public String toString() {
    return String.format("Not(%s)", getExpression());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Not)) {
      return false;
    }

    Not that = (Not) other;
    return Objects.equals(expression, that.expression);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(expression);
  }
}
