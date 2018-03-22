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

import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;

public class PseudoCostCalculator extends DefaultVisitor<Double, Void> {
  public static double calculateCost(Expression expr) {
    PseudoCostCalculator calc = new PseudoCostCalculator();
    return expr.accept(calc, null);
  }

  @Override
  protected Double process(Expression node, Void context) {
    return 1.0;
  }

  @Override
  protected Double visit(LogicalBinaryExpression node, Void context) {
    double leftCost = node.getLeft().accept(this, context);
    double rightCost = node.getLeft().accept(this, context);
    switch (node.getCompType()) {
      case AND:
        return leftCost * rightCost;
      case OR:
      case XOR:
        return leftCost + rightCost;
      default:
        return 1.0;
    }
  }

  @Override
  protected Double visit(ComparisonBinaryExpression node, Void context) {
    switch (node.getComparisonType()) {
      case EQUAL:
        return 0.01;
      case GREATER_EQUAL:
      case GREATER_THAN:
      case LESS_EQUAL:
      case LESS_THAN:
        // magic number for testing
        return 0.3;
      case NOT_EQUAL:
        return 0.99;
      default:
        return 1.0;
    }
  }
}
