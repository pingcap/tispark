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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.types.DataType;
import java.util.List;
import java.util.Objects;

public class AggregateFunction extends Expression {
  private final FunctionType type;
  private final Expression argument;

  private AggregateFunction(FunctionType type, Expression argument, DataType dataType) {
    super(dataType);
    this.type = requireNonNull(type, "function type is null");
    this.argument = requireNonNull(argument, "function argument is null");
  }

  public static AggregateFunction newCall(FunctionType type, Expression argument) {
    return newCall(type, argument, argument.dataType);
  }

  public static AggregateFunction newCall(
      FunctionType type, Expression argument, DataType dataType) {
    return new AggregateFunction(type, argument, dataType);
  }

  public FunctionType getType() {
    return type;
  }

  public Expression getArgument() {
    return argument;
  }

  @Override
  public List<Expression> getChildren() {
    return ImmutableList.of(argument);
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
    if (!(other instanceof AggregateFunction)) {
      return false;
    }

    AggregateFunction that = (AggregateFunction) other;
    return type == that.type && Objects.equals(argument, that.argument);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, argument);
  }

  @Override
  public String toString() {
    return String.format(
        "%s(%s)", getType(), Joiner.on(",").useForNull("NULL").join(getChildren()));
  }

  public enum FunctionType {
    Sum,
    Count,
    Min,
    Max,
    First
  }
}
