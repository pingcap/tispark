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

import com.pingcap.tidb.tipb.ByItem;
import com.pingcap.tikv.expression.visitor.ProtoConverter;
import java.io.Serializable;

public class TiByItem implements Serializable {
  private TiExpr expr;
  private boolean desc;

  public static TiByItem create(TiExpr expr, boolean desc) {
    return new TiByItem(expr, desc);
  }

  private TiByItem(TiExpr expr, boolean desc) {
    checkNotNull(expr, "Expr cannot be null for ByItem");

    this.expr = expr;
    this.desc = desc;
  }

  public ByItem toProto() {
    ByItem.Builder builder = ByItem.newBuilder();
    return builder.setExpr(ProtoConverter.toProto(expr))
                  .setDesc(desc)
                  .build();
  }

  public TiExpr getExpr() {
    return expr;
  }

  @Override
  public String toString() {
    return String.format("[%s %s]", expr.toString(), desc ? "DESC" : "ASC");
  }
}
