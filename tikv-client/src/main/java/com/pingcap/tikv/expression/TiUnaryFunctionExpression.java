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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.pingcap.tikv.types.DataType;

public abstract class TiUnaryFunctionExpression extends TiFunctionExpression {
  private DataType dataType;

  protected TiUnaryFunctionExpression(TiExpr... args) {
    super(args);
  }

  @Override
  protected void validateArguments(TiExpr... args) throws RuntimeException {
    checkNotNull(args, "Arguments of " + getName() + " cannot be null");
    checkArgument(args.length == 1, getName() + " takes only 1 argument");
  }
}
