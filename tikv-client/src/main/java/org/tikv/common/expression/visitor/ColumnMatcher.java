/*
 * Copyright 2018 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.common.expression.visitor;

import org.tikv.common.expression.ColumnRef;
import org.tikv.common.expression.Expression;

public class ColumnMatcher extends DefaultVisitor<Boolean, Void> {
  private final ColumnRef columnRef;

  private ColumnMatcher(ColumnRef exp) {
    this.columnRef = exp;
  }

  public static Boolean match(ColumnRef col, Expression expression) {
    ColumnMatcher matcher = new ColumnMatcher(col);
    return expression.accept(matcher, null);
  }

  @Override
  protected Boolean process(Expression node, Void context) {
    return false;
  }

  @Override
  protected Boolean visit(ColumnRef node, Void context) {
    return node.matchName(columnRef.getName());
  }
}
