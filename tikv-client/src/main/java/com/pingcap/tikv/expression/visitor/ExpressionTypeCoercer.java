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

import static com.pingcap.tikv.types.DecimalType.BIG_INT_DECIMAL;
import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.*;
import com.pingcap.tikv.expression.AggregateFunction.FunctionType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.util.Pair;
import java.util.IdentityHashMap;
import java.util.List;

/**
 * Validate and infer expression type Collected results are returned getTypeMap For now we don't do
 * any type promotion and only coerce from left to right.
 */
public class ExpressionTypeCoercer extends Visitor<Pair<DataType, Double>, DataType> {
  private final IdentityHashMap<Expression, DataType> typeMap = new IdentityHashMap<>();
  private static final double MAX_CREDIBILITY = 1.0;
  private static final double MIN_CREDIBILITY = 0.1;
  private static final double COLUMN_REF_CRED = MAX_CREDIBILITY;
  private static final double CONSTANT_CRED = MIN_CREDIBILITY;
  private static final double LOGICAL_OP_CRED = MAX_CREDIBILITY;
  private static final double COMPARISON_OP_CRED = MAX_CREDIBILITY;
  private static final double STRING_REG_OP_CRED = MAX_CREDIBILITY;
  private static final double FUNCTION_CRED = MAX_CREDIBILITY;
  private static final double ISNULL_CRED = MAX_CREDIBILITY;
  private static final double NOT_CRED = MAX_CREDIBILITY;

  public IdentityHashMap<Expression, DataType> getTypeMap() {
    return typeMap;
  }

  public static DataType inferType(Expression expression) {
    ExpressionTypeCoercer inf = new ExpressionTypeCoercer();
    return inf.infer(expression);
  }

  DataType infer(Expression expression) {
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
  // targetType null means no coerce needed from parent and choose the highest credibility result
  private Pair<DataType, Double> coerceType(DataType targetType, Expression... nodes) {
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
  protected Pair<DataType, Double> visit(StringRegExpression node, DataType targetType) {
    if (targetType != null && !targetType.equals(IntegerType.BOOLEAN)) {
      throw new TiExpressionException(String.format("StringReg result cannot be %s", targetType));
    }
    if (!typeMap.containsKey(node)) {
      coerceType(null, node.getLeft(), node.getRight());
      typeMap.put(node, IntegerType.BOOLEAN);
    }
    return Pair.create(IntegerType.BOOLEAN, STRING_REG_OP_CRED);
  }

  @Override
  protected Pair<DataType, Double> visit(ArithmeticBinaryExpression node, DataType targetType) {
    if (node.isResolved()) {
      typeMap.put(node, node.dataType);
      return Pair.create(node.dataType, MAX_CREDIBILITY);
    } else {
      Pair<DataType, Double> result = coerceType(targetType, node.getLeft(), node.getRight());
      typeMap.put(node, result.first);
      return result;
    }
  }

  @Override
  protected Pair<DataType, Double> visit(LogicalBinaryExpression node, DataType targetType) {
    if (targetType != null && !targetType.equals(IntegerType.BOOLEAN)) {
      throw new TiExpressionException(
          String.format("LogicalBinary result cannot be %s", targetType));
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
    coerceType(null, node.getArgument());
    switch (fType) {
      case Count:
        {
          if (targetType != null && targetType.equals(IntegerType.BIGINT)) {
            throw new TiExpressionException(String.format("Count cannot be %s", targetType));
          }
          typeMap.put(node, IntegerType.BIGINT);
          return Pair.create(targetType, FUNCTION_CRED);
        }
      case Sum:
        {
          // TODO: this is used to bybass sum(tp_decimal) promotion
          // we will fix this later.
          if (node.isResolved()) {
            typeMap.put(node, node.getDataType());
            return Pair.create(node.getDataType(), FUNCTION_CRED);
          } else {
            if (targetType instanceof DecimalType) {
              throw new TiExpressionException(String.format("Sum cannot be %s", targetType));
            }
            DataType colType = node.getArgument().accept(this, null).first;
            if (colType instanceof RealType) {
              typeMap.put(node, RealType.DOUBLE);
            } else if (colType instanceof DecimalType) {
              typeMap.put(node, colType);
            } else {
              typeMap.put(node, BIG_INT_DECIMAL);
            }
            return Pair.create(targetType, FUNCTION_CRED);
          }
        }
      case First:
      case Max:
      case Min:
        {
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

  @Override
  protected Pair<DataType, Double> visit(Not node, DataType targetType) {
    if (targetType != null && !targetType.equals(IntegerType.BOOLEAN)) {
      throw new TiExpressionException(String.format("Not result cannot be %s", targetType));
    }
    if (!typeMap.containsKey(node)) {
      coerceType(null, node.getExpression());
      typeMap.put(node, IntegerType.BOOLEAN);
    }
    return Pair.create(IntegerType.BOOLEAN, NOT_CRED);
  }

  @Override
  protected Pair<DataType, Double> visit(FuncCallExpr node, DataType targetType) {
    switch (node.getFuncTp()) {
      case YEAR:
        if (targetType != null && !targetType.equals(IntegerType.INT)) {
          throw new TiExpressionException(
              String.format("FuncCallExpr result cannot be %s", targetType));
        }
        if (!typeMap.containsKey(node)) {
          coerceType(null, node.getExpression());
          typeMap.put(node, IntegerType.INT);
        }
        return Pair.create(IntegerType.INT, FUNCTION_CRED);
      default:
        throw new TiExpressionException(
            String.format("FuncCallExpr result cannot be %s", targetType));
    }
  }
}
