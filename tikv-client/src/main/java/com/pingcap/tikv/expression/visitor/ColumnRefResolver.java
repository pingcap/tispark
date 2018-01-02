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

package com.pingcap.tikv.expression.visitor;


import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.Visitor;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.List;
import java.util.Objects;

public class ColumnRefResolver extends Visitor<Void, Void> {
  public static void resolve(TiExpr expression, TiTableInfo table) {
    ColumnRefResolver resolver = new ColumnRefResolver(table);
    resolver.resolve(expression);
  }

  public static void resolve(List<? extends TiExpr> expressions, TiTableInfo table) {
    ColumnRefResolver resolver = new ColumnRefResolver(table);
    resolver.resolve(expressions);
  }

  private final TiTableInfo table;

  public ColumnRefResolver(TiTableInfo table) {
    this.table = table;
  }

  public void resolve(List<? extends TiExpr> expressions) {
    expressions.forEach(expression -> expression.accept(this, null));
  }

  public void resolve(TiExpr expression) {
    Objects.requireNonNull(expression, "expression is null");
    expression.accept(this, null);
  }

  @Override
  protected Void visit(TiColumnRef node, Void context) {
    node.resolve(table);
    return null;
  }
}
