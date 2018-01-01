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

import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.expression.scalar.And;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PredicateUtils {
  public static TiExpr mergeCNFExpressions(List<TiExpr> exprs) {
    requireNonNull(exprs);
    if (exprs.size() == 0) return null;
    if (exprs.size() == 1) return exprs.get(0);

    return new And(exprs.get(0), mergeCNFExpressions(exprs.subList(1, exprs.size())));
  }

  public static Set<TiColumnRef> extractColumnRefFromExpr(TiExpr expr) {
    Set<TiColumnRef> columnRefs = new HashSet<>();
    if (expr instanceof TiFunctionExpression) {
      TiFunctionExpression tiF = (TiFunctionExpression) expr;
      for (TiExpr arg : tiF.getArgs()) {
        if (arg instanceof TiColumnRef) {
          TiColumnRef tiCR = (TiColumnRef) arg;
          columnRefs.add(tiCR);
        }
      }
    } else if (expr instanceof TiColumnRef) {
      columnRefs.add(((TiColumnRef) expr));
    }
    return columnRefs;
  }
}
