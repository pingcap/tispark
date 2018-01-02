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


import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.ArithmeticBinaryExpression;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FunctionCall;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Visitor;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Validate and infer expression type
 * Collected results are returned getTypeMap
 */
public class ExpressionTypeInferrer extends Visitor<DataType, Void> {
  private final IdentityHashMap<Expression, DataType> typeMap = new IdentityHashMap<>();

  public IdentityHashMap<Expression, DataType> getTypeMap() {
    return typeMap;
  }

  public DataType infer(Expression expression) {
    requireNonNull(expression, "expression is null");
    return expression.accept(this, null);
  }

  public void infer(List<? extends Expression> expressions) {
    requireNonNull(expressions, "expressions is null");
    expressions.forEach(expr -> expr.accept(this, null));
  }

  @Override
  protected DataType visit(ColumnRef node, Void context) {
    DataType type = node.getType();
    typeMap.put(node, type);
    return type;
  }

  protected DataType verifySameType(Void context, Expression...nodes) {
    if (nodes.length == 0) {
      throw new TiExpressionException("failed to verify empty node list");
    }
    DataType baseline = nodes[0].accept(this, context);
    for (int i = 1; i < nodes.length; i++) {
      DataType curType = nodes[i].accept(this, context);
      if (!baseline.equals(curType)) {
        throw new TiExpressionException(String.format("cannot compare different types %s vs %s", baseline, curType));
      }
    }
    return baseline;
  }

  @Override
  protected DataType visit(ComparisonBinaryExpression node, Void context) {
    verifySameType(context, node.getLeft(), node.getRight());
    typeMap.put(node, IntegerType.BOOLEAN);
    return IntegerType.BOOLEAN;
  }

  @Override
  protected DataType visit(ArithmeticBinaryExpression node, Void context) {
    DataType type = verifySameType(context, node.getLeft(), node.getRight());
    typeMap.put(node, type);
    return type;
  }

  @Override
  protected DataType visit(LogicalBinaryExpression node, Void context) {
    DataType type = verifySameType(context, node.getLeft(), node.getRight());
    typeMap.put(node, type);
    return IntegerType.BOOLEAN;
  }

  @Override
  protected DataType visit(Constant node, Void context) {
    DataType type = node.getType();
    typeMap.put(node, type);
    return type;
  }

  @Override
  protected DataType visit(FunctionCall node, Void context) {
    return null;
  }
}
