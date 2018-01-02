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

import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.FunctionCall;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Visitor;

public class SupportedExpressionValidator extends Visitor<Boolean, Void> {

  @Override
  protected Boolean visit(ColumnRef node, Void context) {
    return null;
  }

  @Override
  protected Boolean visit(ComparisonBinaryExpression node, Void context) {
    return null;
  }

  @Override
  protected Boolean visit(ArithmeticBinaryExpression node, Void context) {
    return null;
  }

  @Override
  protected Boolean visit(LogicalBinaryExpression node, Void context) {
    return null;
  }

  @Override
  protected Boolean visit(Constant node, Void context) {
    return null;
  }

  @Override
  protected Boolean visit(FunctionCall node, Void context) {
    return null;
  }
}
