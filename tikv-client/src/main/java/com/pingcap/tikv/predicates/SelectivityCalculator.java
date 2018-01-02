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


import com.pingcap.tikv.expression.ComparisonExpression;
import com.pingcap.tikv.expression.TiExpr;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class SelectivityCalculator {
  public static final double SELECTION_FACTOR = 100;
  public static final double EQUAL_RATE = 0.01;
  public static final double LESS_RATE = 0.1;

  public static double calcPseudoSelectivity(ScanSpec spec) {
    List<TiExpr> exprs = new ArrayList<>();
    exprs.addAll(spec.getPointPredicates());
    Optional<TiExpr> rangePred = spec.getRangePredicate();
    rangePred.map(x -> exprs.add(x));
    double minFactor = SELECTION_FACTOR;
    for (TiExpr expr : exprs) {
      if (expr instanceof ComparisonExpression) {
        ComparisonExpression compExpression = (ComparisonExpression) expr;
        switch (compExpression.getComparisonType()) {
          case EQUAL:
            minFactor *= EQUAL_RATE;
            break;
          default:
            minFactor *= LESS_RATE;
        }
      }
    }
    return minFactor;
  }
}
