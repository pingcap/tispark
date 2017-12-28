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


import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.scalar.Equal;
import com.pingcap.tikv.expression.scalar.GreaterEqual;
import com.pingcap.tikv.expression.scalar.GreaterThan;
import com.pingcap.tikv.expression.scalar.LessEqual;
import com.pingcap.tikv.expression.scalar.LessThan;
import com.pingcap.tikv.expression.scalar.NullEqual;

public class SelectivityCalculator {
  public static final double SELECTION_FACTOR = 100;
  public static final double EQUAL_RATE = 0.01;
  public static final double LESS_RATE = 0.1;

  public static double calcPseudoSelectivity(Iterable<TiExpr> exprs) {
    double minFactor = SELECTION_FACTOR;
    for (TiExpr expr : exprs) {
      if (expr instanceof Equal || expr instanceof NullEqual) {
        minFactor *= EQUAL_RATE;
      } else if (
          expr instanceof GreaterEqual ||
          expr instanceof GreaterThan ||
          expr instanceof LessEqual ||
          expr instanceof LessThan) {
        minFactor *= LESS_RATE;
      }
    }
    return minFactor;
  }
}
