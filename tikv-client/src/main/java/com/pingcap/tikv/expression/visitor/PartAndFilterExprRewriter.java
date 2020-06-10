/*
 * Copyright 2020 PingCAP, Inc.
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
import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.ComparisonBinaryExpression.NormalizedPredicate;
import com.pingcap.tikv.expression.Constant;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.FuncCallExpr;
import com.pingcap.tikv.expression.FuncCallExpr.Type;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.predicates.PredicateUtils;
import java.util.Objects;
import java.util.Set;

/**
 * PartAndFilterExprRewriter takes partition expression as an input. Rewriting rule is based on the
 * type of partition expression. 1. If partition expression is a columnRef, no rewriting will be
 * performed. 2. If partition expression is year and the expression to be rewritten in the form of y
 * < '1995-10-10' then its right hand child will be replaced with "1995". 3. If partition expression
 * is year and the expression to be rewritten in the form of year(y) < '1995' then its left hand
 * child will be replaced with y.
 */
public class PartAndFilterExprRewriter extends DefaultVisitor<Expression, Void> {
  private final Expression partExpr;
  private final Set<ColumnRef> columnRefs;

  private boolean unsupportedPartFnFound;

  public PartAndFilterExprRewriter(Expression partExpr) {
    Objects.requireNonNull(partExpr, "partition expression cannot be null");
    this.partExpr = partExpr;
    this.columnRefs = PredicateUtils.extractColumnRefFromExpression(partExpr);
  }

  private boolean isYear() {
    return partExpr instanceof FuncCallExpr && ((FuncCallExpr) partExpr).getFuncTp() == Type.YEAR;
  }

  private boolean isColumnRef() {
    return partExpr instanceof ColumnRef;
  }

  @Override
  protected Expression process(Expression node, Void context) {
    for (Expression expr : node.getChildren()) {
      expr.accept(this, context);
    }
    return node;
  }

  public Expression visit(LogicalBinaryExpression node, Void context) {
    Expression left = node.getLeft().accept(this, null);
    Expression right = node.getRight().accept(this, null);
    return new LogicalBinaryExpression(node.getCompType(), left, right);
  }

  @Override
  public Expression visit(FuncCallExpr node, Void context) {
    if (node.getFuncTp() == Type.YEAR) {
      return node.getExpression();
    }
    // other's is not supported right now.
    // TODO: when adding new type in FuncCallExpr, please also modify here
    // accordingly.
    return node;
  }

  @Override
  public Expression visit(Constant node, Void context) {
    return node;
  }

  @Override
  public Expression visit(ComparisonBinaryExpression node, Void context) {
    NormalizedPredicate predicate = node.normalize();
    // predicate maybe null if node's left or right does not have a column ref or a constant.
    if (predicate != null) {
      if (!columnRefs.contains(predicate.getColumnRef())) {
        return node;
      }
      // we only support year for now.
      if (isYear()) {
        FuncCallExpr year = new FuncCallExpr(predicate.getValue(), Type.YEAR);
        Constant newLiteral = year.eval(predicate.getValue());
        return new ComparisonBinaryExpression(node.getComparisonType(), node.getLeft(), newLiteral);
      } else if (isColumnRef()) {
        return node;
      }
      unsupportedPartFnFound = true;
      return null;
    }

    // when we find a node in form like [year(y) < 1995], we need rewrite the left child.
    Expression left = node.getLeft().accept(this, null);
    Expression right = node.getRight().accept(this, null);
    return new ComparisonBinaryExpression(node.getComparisonType(), left, right);
  }

  public Expression rewrite(Expression target) {
    return target.accept(this, null);
  }

  public boolean isUnsupportedPartFnFound() {
    return unsupportedPartFnFound;
  }
}
