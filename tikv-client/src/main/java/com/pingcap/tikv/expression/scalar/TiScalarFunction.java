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

import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tidb.tipb.FieldType;
import com.pingcap.tidb.tipb.ScalarFuncSig;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.ScalarFuncInfer;

/**
 * Scalar function
 * Used in DAG mode
 */
public abstract class TiScalarFunction extends TiFunctionExpression {
  TiScalarFunction(TiExpr... args) {
    super(args);
  }

  /**
   * Gets scalar function PB code representation.
   *
   * @return the pb code
   */
  ScalarFuncSig getSignature() {
    return ScalarFuncInfer.of(
        getArgTypeCode(),
        getExprType()
    );
  }

  @Override
  public String getName() {
    return getSignature().name();
  }

  private DataType getArgType() {
    if (args.isEmpty()) {
      throw new TiExpressionException(
          "Scalar function's argument list cannot be empty!"
      );
    }

    return args.get(0).getType();
  }

  /**
   * Get scalar function argument type code
   * Note:In DAG mode, all the arguments' type should
   * be the same
   *
   * @return the arg type code
   */
  public Integer getArgTypeCode() {
    return getArgType().getTypeCode();
  }

  @Override
  public DataType getType() {
    return getArgType();
  }

  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();
    // Scalar function type
    builder.setTp(ExprType.ScalarFunc);
    // Return type
    builder.setFieldType(
        FieldType.newBuilder()
            .setTp(
                getType().getTypeCode()
            )
            .build()
    );
    // Set function signature
    builder.setSig(getSignature());
    for (TiExpr arg : args) {
      builder.addChildren(arg.toProto());
    }

    return builder.build();
  }
}
