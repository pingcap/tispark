/*
 *
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
 *
 */

package com.pingcap.tikv.expression;


public abstract class Visitor<R, C> {
  abstract protected R visit(ColumnRef node, C context);

  abstract protected R visit(ComparisonBinaryExpression node, C context);

  abstract protected R visit(ArithmeticBinaryExpression node, C context);

  abstract protected R visit(LogicalBinaryExpression node, C context);

  abstract protected R visit(Constant node, C context);

  abstract protected R visit(AggregateFunction node, C context);

  abstract protected R visit(IsNull node, C context);

  abstract protected R visit(Not node, C context);
}
