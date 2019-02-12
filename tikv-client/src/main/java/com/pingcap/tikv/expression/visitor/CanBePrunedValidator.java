/*
 * Copyright 2019 PingCAP, Inc.
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
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.LogicalBinaryExpression.Type;
import com.pingcap.tikv.expression.visitor.CanBePrunedValidator.ContextPartExpr;
import java.util.HashSet;
import java.util.Set;

public class CanBePrunedValidator extends DefaultVisitor<Void, ContextPartExpr> {
  public static class ContextPartExpr {
    Expression partExpr;
    boolean foundOR;
    boolean orWithUnrelatedCol;
    Set<Expression> seenCOlRefs;

    public ContextPartExpr(Expression partExpr) {
      this.partExpr = partExpr;
      this.foundOR = false;
      this.orWithUnrelatedCol = false;
      this.seenCOlRefs = new HashSet<>();
    }
  }

  private static final CanBePrunedValidator validator = new CanBePrunedValidator();

  public static boolean canBePruned(Expression node, ContextPartExpr contextPartExpr) {
    node.accept(validator, contextPartExpr);
    if (contextPartExpr.orWithUnrelatedCol) {
      return false;
    }
    if (contextPartExpr.seenCOlRefs != null) {
      return contextPartExpr.seenCOlRefs.contains(contextPartExpr.partExpr);
    } else {
      return false;
    }
  }

  @Override
  protected Void visit(LogicalBinaryExpression lbe, ContextPartExpr contextPartExpr) {
    if (lbe.getCompType() == Type.OR) {
      contextPartExpr.foundOR = true;
    }
    process(lbe, contextPartExpr);
    return null;
  }

  @Override
  protected Void visit(ColumnRef colRef, ContextPartExpr contextPartExpr) {
    if (contextPartExpr.foundOR) {
      contextPartExpr.orWithUnrelatedCol = !colRef.equals(contextPartExpr.partExpr);
    }
    contextPartExpr.seenCOlRefs.add(colRef);
    return null;
  }

  @Override
  protected Void process(Expression node, ContextPartExpr partExpr) {
    for (Expression expr : node.getChildren()) {
      expr.accept(this, partExpr);
    }
    return null;
  }
}
