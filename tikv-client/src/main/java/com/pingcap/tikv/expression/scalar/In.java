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

package com.pingcap.tikv.expression.scalar;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;

public class In extends TiScalarFunction {
  public In(TiExpr... args) {
    super(args);
  }

  @Override
  protected ExprType getExprType() {
    return ExprType.In;
  }

  @Override
  protected void validateArguments(TiExpr... args) throws RuntimeException {
    requireNonNull(args, "Expressions cannot be null");
    checkArgument(args.length >= 2, "Value list cannot be empty for In Expression");
  }

  @Override
  public DataType getType() {
    return IntegerType.DEF_BOOLEAN_TYPE;
  }
}
