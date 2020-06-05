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

import static com.google.common.base.Preconditions.checkNotNull;

import com.pingcap.tikv.expression.visitor.ProtoConverter;
import java.io.Serializable;

public class ByItem implements Serializable {
  private final Expression expr;
  private final boolean desc;

  private ByItem(Expression expr, boolean desc) {
    checkNotNull(expr, "Expr cannot be null for ByItem");

    this.expr = expr;
    this.desc = desc;
  }

  public static ByItem create(Expression expr, boolean desc) {
    return new ByItem(expr, desc);
  }

  public com.pingcap.tidb.tipb.ByItem toProto(Object context) {
    com.pingcap.tidb.tipb.ByItem.Builder builder = com.pingcap.tidb.tipb.ByItem.newBuilder();
    return builder.setExpr(ProtoConverter.toProto(expr, context)).setDesc(desc).build();
  }

  public Expression getExpr() {
    return expr;
  }

  public boolean isDesc() {
    return desc;
  }

  @Override
  public String toString() {
    return String.format("[%s %s]", expr.toString(), desc ? "DESC" : "ASC");
  }
}
