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

import com.pingcap.tikv.expression.*;

public class DefaultVisitor<R, C> extends Visitor<R, C> {
  protected R process(Expression node, C context) {
    for (Expression expr : node.getChildren()) {
      expr.accept(this, context);
    }
    return null;
  }

  @Override
  protected R visit(ColumnRef node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(ComparisonBinaryExpression node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(StringRegExpression node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(ArithmeticBinaryExpression node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(LogicalBinaryExpression node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(Constant node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(AggregateFunction node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(IsNull node, C context) {
    return process(node, context);
  }

  @Override
  protected R visit(Not node, C context) {
    return process(node, context);
  }
}
