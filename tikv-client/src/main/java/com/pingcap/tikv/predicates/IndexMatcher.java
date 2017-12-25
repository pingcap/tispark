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

import static com.google.common.base.Preconditions.checkNotNull;

import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiConstant;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.*;
import com.pingcap.tikv.meta.TiIndexColumn;

public class IndexMatcher {
  private final boolean matchEqualTestOnly;
  private final TiIndexColumn indexColumn;

  protected IndexMatcher(TiIndexColumn indexColumn, boolean matchEqualTestOnly) {
    this.matchEqualTestOnly = matchEqualTestOnly;
    this.indexColumn = indexColumn;
  }

  /**
   * Test if a filter expression matches an index It does only if it's an comparision-style filter
   *
   * @param expr to match
   * @return if a expr matches the index
   */
  public boolean match(TiExpr expr) {
    checkNotNull(expr, "Expression should not be null");

    if (expr instanceof TiFunctionExpression) {
      return matchFunction((TiFunctionExpression) expr);
    } else if (expr instanceof TiColumnRef) {
      return matchColumn((TiColumnRef) expr);
    } else if (expr instanceof TiConstant) {
      return true;
    }
    throw new TiClientInternalException(
        "Invalid Type for condition checker: " + expr.getClass().getSimpleName());
  }

  private boolean matchFunction(TiFunctionExpression expr) {
    try {
      if (expr instanceof And || expr instanceof Or) {
        // If we pick eq only, AND is not allowed
        return !matchEqualTestOnly || !(expr instanceof And) && match(expr.getArg(0)) && match(
            expr.getArg(1));
      } else {
        if (matchEqualTestOnly) {
          if (!(expr instanceof Equal) && !(expr instanceof In)) {
            return false;
          }
        }
        AccessConditionNormalizer.NormalizedCondition cond =
            AccessConditionNormalizer.normalize(expr);

        return matchColumn(cond.columnRef);
      }
    } catch (Exception e) {
      return false;
    }
  }

  private boolean matchColumn(TiColumnRef col) {
    if (indexColumn != null) {
      String indexColumnName = indexColumn.getName();
      return col.getColumnInfo().matchName(indexColumnName);
    }
    return false;
  }
}
