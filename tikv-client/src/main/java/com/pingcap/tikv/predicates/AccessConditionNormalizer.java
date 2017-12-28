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

package com.pingcap.tikv.predicates;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.*;
import java.util.List;

public class AccessConditionNormalizer {
  /**
   * Validate an expression that if it is a valid predicate for access condition An access condition
   * is an expression can be turn into access point or range
   *
   * @param expr expression to test
   * @return false if such expression is not validated.
   */
  public static boolean validate(TiExpr expr) {
    if (expr instanceof TiFunctionExpression) {
      TiFunctionExpression func = (TiFunctionExpression) expr;
      if (func instanceof GreaterThan
          || func instanceof GreaterEqual
          || func instanceof LessThan
          || func instanceof LessEqual
          || func instanceof Equal
          || func instanceof NotEqual) {
        if ((func.getArg(0) instanceof TiColumnRef && func.getArg(1) instanceof TiConstant)
            || (func.getArg(1) instanceof TiColumnRef && func.getArg(0) instanceof TiConstant)) {
          return true;
        }
      }
      if (func instanceof In) {
        if (func.getArg(0) instanceof TiColumnRef) {
          for (int i = 1; i < func.getArgSize(); i++) {
            if (!(func.getArg(i) instanceof TiConstant)) {
              return false;
            }
          }
          return true;
        }
      }
    }
    return false;
  }

  private static TiFunctionExpression normalizeComparision(TiFunctionExpression expr) {
    if (expr.getArg(0) instanceof TiConstant && expr.getArg(1) instanceof TiColumnRef) {
      // Reverse condition if lhs is constant
      if (expr instanceof GreaterThan) {
        return new LessThan(expr.getArg(1), expr.getArg(0));
      } else if (expr instanceof GreaterEqual) {
        return new LessEqual(expr.getArg(1), expr.getArg(0));
      } else if (expr instanceof LessThan) {
        return new GreaterThan(expr.getArg(1), expr.getArg(0));
      } else if (expr instanceof LessEqual) {
        return new GreaterEqual(expr.getArg(1), expr.getArg(0));
      } else if (expr instanceof Equal) {
        return new Equal(expr.getArg(1), expr.getArg(0));
      } else if (expr instanceof NotEqual) {
        return new NotEqual(expr.getArg(1), expr.getArg(0));
      }
    }
    return expr;
  }

  public static class NormalizedCondition {
    final TiColumnRef columnRef;
    final List<TiConstant> constantVals;
    public final TiFunctionExpression condition;

    NormalizedCondition(
        TiColumnRef columnRef, List<TiConstant> constantVals, TiFunctionExpression condition) {
      this.columnRef = columnRef;
      this.constantVals = constantVals;
      this.condition = condition;
    }
  }

  static NormalizedCondition normalize(TiExpr expr) {
    if (!validate(expr)) {
      throw new TiClientInternalException("Not a valid access condition expression: " + expr);
    }

    if (expr instanceof TiFunctionExpression) {
      TiFunctionExpression func = (TiFunctionExpression) expr;
      func = normalizeComparision(func);

      if (func instanceof In) {
        ImmutableList.Builder<TiConstant> vals = ImmutableList.builder();
        for (int i = 1; i < func.getArgSize(); i++) {
          // Checked already
          vals.add((TiConstant) func.getArg(i));
        }
        return new NormalizedCondition((TiColumnRef) func.getArg(0), vals.build(), func);
      } else {
        return new NormalizedCondition(
            (TiColumnRef) func.getArg(0), ImmutableList.of((TiConstant) func.getArg(1)), func);
      }
    }
    throw new TiClientInternalException("Not a valid access condition expression: " + expr);
  }
}
