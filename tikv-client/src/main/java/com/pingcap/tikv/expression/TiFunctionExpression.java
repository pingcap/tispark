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
import static java.util.Objects.requireNonNull;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.Expr;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.List;


public abstract class TiFunctionExpression implements TiExpr {

  protected final List<TiExpr> args;

  protected TiFunctionExpression(TiExpr... args) {
    this.args = ImmutableList.copyOf(args);
    validateArguments(args);
  }

  protected abstract ExprType getExprType();

  public TiExpr getArg(int i) {
    checkArgument(i < args.size(), "Index out of bound for TiExpression Arguments");
    return args.get(i);
  }

  public int getArgSize() {
    return args.size();
  }

  public List<TiExpr> getArgs() {
    return args;
  }

  @Override
  public Expr toProto() {
    Expr.Builder builder = Expr.newBuilder();

    builder.setTp(getExprType());

    for (TiExpr arg : args) {
      builder.addChildren(arg.toProto());
    }

    return builder.build();
  }

  public String getName() {
    return getClass().getSimpleName();
  }

  protected void validateArguments(TiExpr... args) throws RuntimeException {
    requireNonNull(args, "Expressions cannot be null");
    for (TiExpr expr : args) {
      requireNonNull(expr, "Expressions cannot be null.");
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) return false;
    if (this.getClass().equals(other.getClass())) {
      TiFunctionExpression func = (TiFunctionExpression) other;
      for (int i = 0; i < func.getArgSize(); i++) {
        TiExpr arg = func.getArg(i);
        if (!getArg(i).equals(arg)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }

  @Override
  public boolean isSupportedExpr(ExpressionBlacklist blackList) {
    for (TiExpr arg : args) {
      if (!arg.isSupportedExpr(blackList)) {
        return false;
      }
    }
    return TiExpr.super.isSupportedExpr(blackList);
  }

  @Override
  public int hashCode() {
    int hash = 31 * getClass().hashCode();
    for (TiExpr arg : args) {
      hash *= arg.hashCode();
    }
    return hash;
  }

  @Override
  public TiFunctionExpression resolve(TiTableInfo table) {
    for (TiExpr arg : args) {
      arg.resolve(table);
    }
    return this;
  }

  @Override
  public String toString() {
    return String.format("%s(%s)",
                         getName(),
                         Joiner.on(", ").skipNulls().join(args));
  }
}
