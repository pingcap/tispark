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
import com.pingcap.tikv.expression.AggregateFunction;
import com.pingcap.tikv.expression.AggregateFunction.FunctionType;
import com.pingcap.tikv.expression.IsNull;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.Visitor;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.Pair;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Validate and infer expression type
 * Collected results are returned getTypeMap
 * For now we don't do any type promotion and only coerce from left to right.
 */
public class ExpressionTypeCoercer extends Visitor<Pair<DataType, Double>, DataType> {
  private final IdentityHashMap<Expression, DataType> typeMap = new IdentityHashMap<>();
  private final static double MAX_CREDIBILITY = 1.0;
  private final static double MIN_CREDIBILITY = 0.1;
  private final static double COLUMN_REF_CRED = MAX_CREDIBILITY;
  private final static double CONSTANT_CRED = MIN_CREDIBILITY;
  private final static double LOGICAL_OP_CRED = MAX_CREDIBILITY;
  private final static double COMPARISON_OP_CRED = MAX_CREDIBILITY;
  private final static double FUNCTION_CRED = MAX_CREDIBILITY;
  private final static double ISNULL_CRED = MAX_CREDIBILITY;

  public IdentityHashMap<Expression, DataType> getTypeMap() {
    return typeMap;
  }

  public static DataType inferType(Expression expression) {
    ExpressionTypeCoercer inf = new ExpressionTypeCoercer();
    return inf.infer(expression);
  }

  public DataType infer(Expression expression) {
    requireNonNull(expression, "expression is null");
    return expression.accept(this, null).first;
  }

  public void infer(List<? extends Expression> expressions) {
    requireNonNull(expressions, "expressions is null");
    expressions.forEach(expr -> expr.accept(this, null));
  }

  @Override
  protected Pair<DataType, Double> visit(ColumnRef node, DataType targetType) {
    DataType type = node.getType();
    if (targetType != null && !targetType.equals(type)) {
      throw new TiExpressionException(String.format("Column %s cannot be %s", node, targetType));
    }
    typeMap.put(node, type);
    return Pair.create(type, COLUMN_REF_CRED);
  }

  // Try to coerceType if needed
  // A column reference is source of coerce and constant is the subject to coerce
  protected Pair<DataType, Double> coerceType(DataType targetType, Expression...nodes) {
    if (nodes.length == 0) {
      throw new TiExpressionException("failed to verify empty node list");
    }
    if (targetType == null) {
      Pair<DataType, Double> baseline = nodes[0].accept(this, null);
      for (int i = 1; i < nodes.length; i++) {
        Pair<DataType, Double> current = nodes[i].accept(this, null);
        if (current.second > baseline.second) {
          baseline = current;
        }
      }
      for (Expression node : nodes) {
        node.accept(this, baseline.first);
      }
      return baseline;
    } else {
      double credibility = -1;
      for (Expression node : nodes) {
        Pair<DataType, Double> result = node.accept(this, targetType);
        if (result.second > credibility) {
          credibility = result.second;
        }
      }
      return Pair.create(targetType, credibility);
    }
  }

  @Override
  protected Pair<DataType, Double> visit(ComparisonBinaryExpression node, DataType targetType) {
    if (targetType != null && !targetType.equals(IntegerType.BOOLEAN)) {
      throw new TiExpressionException(String.format("Comparison result cannot be %s", targetType));
    }
    if (!typeMap.containsKey(node)) {
      coerceType(null, node.getLeft(), node.getRight());
      typeMap.put(node, IntegerType.BOOLEAN);
    }
    return Pair.create(IntegerType.BOOLEAN, COMPARISON_OP_CRED);
  }

  @Override
  protected Pair<DataType, Double> visit(ArithmeticBinaryExpression node, DataType targetType) {
    Pair<DataType, Double> result = coerceType(targetType, node.getLeft(), node.getRight());
    typeMap.put(node, result.first);
    return result;
  }

  @Override
  protected Pair<DataType, Double> visit(LogicalBinaryExpression node, DataType targetType) {
    if (targetType != null && !targetType.equals(IntegerType.BOOLEAN)) {
      throw new TiExpressionException(String.format("Comparison result cannot be %s", targetType));
    }
    if (!typeMap.containsKey(node)) {
      coerceType(null, node.getLeft(), node.getRight());
      typeMap.put(node, IntegerType.BOOLEAN);
    }
    return Pair.create(IntegerType.BOOLEAN, LOGICAL_OP_CRED);
  }

  @Override
  protected Pair<DataType, Double> visit(Constant node, DataType targetType) {
    if (targetType == null) {
      return Pair.create(node.getType(), CONSTANT_CRED);
    } else {
      node.setType(targetType);
      typeMap.put(node, targetType);
      return Pair.create(targetType, CONSTANT_CRED);
    }
  }

  @Override
  protected Pair<DataType, Double> visit(AggregateFunction node, DataType targetType) {
    FunctionType fType = node.getType();
    switch (fType) {
      case Count:
      case Sum: {
        if (targetType != null && targetType.equals(DecimalType.DECIMAL)) {
          throw new TiExpressionException(String.format("Count cannot be %s", targetType));
        }
        typeMap.put(node, DecimalType.DECIMAL);
        return Pair.create(targetType, FUNCTION_CRED);
      }
      case First:
      case Max:
      case Min: {
        Pair<DataType, Double> result = coerceType(targetType, node.getArgument());
        typeMap.put(node, result.first);
        return result;
      }
      default:
        throw new TiExpressionException(String.format("Unknown function %s", fType));
    }
  }

  @Override
  protected Pair<DataType, Double> visit(IsNull node, DataType targetType) {
    if (targetType != null && !targetType.equals(IntegerType.BOOLEAN)) {
      throw new TiExpressionException(String.format("IsNull result cannot be %s", targetType));
    }
    if (!typeMap.containsKey(node)) {
      coerceType(null, node.getExpression());
      typeMap.put(node, IntegerType.BOOLEAN);
    }
    return Pair.create(IntegerType.BOOLEAN, ISNULL_CRED);
  }
}
