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

import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.meta.TiTableInfo;
import java.util.List;
import java.util.Objects;

@Deprecated
public class MetaResolver extends DefaultVisitor<Void, Expression> {
  public static void resolve(Expression expression, TiTableInfo table) {
    MetaResolver resolver = new MetaResolver(table);
    resolver.resolve(expression);
  }

  public static void resolve(List<? extends Expression> expressions, TiTableInfo table) {
    MetaResolver resolver = new MetaResolver(table);
    resolver.resolve(expressions);
  }

  private final TiTableInfo table;

  public MetaResolver(TiTableInfo table) {
    this.table = table;
  }

  public void resolve(List<? extends Expression> expressions) {
    expressions.forEach(expression -> expression.accept(this, null));
  }

  public void resolve(Expression expression) {
    Objects.requireNonNull(expression, "expression is null");
    expression.accept(this, null);
  }

  @Override
  protected Void visit(ColumnRef node, Expression parent) {
    node.resolve(table);
    return null;
  }
}
