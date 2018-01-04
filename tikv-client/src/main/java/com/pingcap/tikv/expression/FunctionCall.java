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
import java.util.List;

public class FunctionCall implements Expression {
  private final String name;
  private final List<Expression> arguments;

  public static FunctionCall newCall(String name, Expression...args) {
    return new FunctionCall(name, args);
  }

  private FunctionCall(String name, Expression[] arguments) {
    this.name = requireNonNull(name, "function name is null").toLowerCase();
    this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "function argument is null"));
  }

  public String getName() {
    return name;
  }

  @Override
  public List<Expression> getChildren() {
    return arguments;
  }

  @Override
  public <R, C> R accept(Visitor<R, C> visitor, C context) {
    return visitor.visit(this, context);
  }
}
