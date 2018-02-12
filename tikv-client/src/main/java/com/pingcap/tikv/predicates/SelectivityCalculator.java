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


import com.pingcap.tikv.expression.ComparisonBinaryExpression;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.LogicalBinaryExpression;
import com.pingcap.tikv.expression.visitor.DefaultVisitor;
import com.pingcap.tikv.expression.visitor.PseudoCostCalculator;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.statistics.ColumnStatistics;
import com.pingcap.tikv.statistics.TableStatistics;

import java.util.BitSet;
import java.util.List;
import java.util.Optional;

import static com.pingcap.tikv.predicates.PredicateUtils.expressionToIndexRanges;

public class SelectivityCalculator extends DefaultVisitor<Double, TableStatistics> {
  public static double calcPseudoSelectivity(ScanSpec spec) {
    Optional<Expression> rangePred = spec.getRangePredicate();
    double cost = 100.0;
    if (spec.getPointPredicates() != null) {
      for (Expression expr : spec.getPointPredicates()) {
        cost *= PseudoCostCalculator.calculateCost(expr);
      }
    }
    if (rangePred.isPresent()) {
      return PseudoCostCalculator.calculateCost(rangePred.get());
    }
    return cost;
  }

  @Override
  protected Double process(Expression node, TableStatistics context) {
    return 1.0;
  }

  @Override
  protected Double visit(LogicalBinaryExpression node, TableStatistics context) {
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
  protected Double visit(ComparisonBinaryExpression node, TableStatistics context) {
    switch (node.getComparisonType()) {
      case EQUAL:
        ComparisonBinaryExpression.NormalizedPredicate predicate = node.normalize();
        if (predicate == null) {
          return 1.0;
        }
        TiColumnInfo columnInfo = predicate.getColumnRef().getColumnInfo();
        ColumnStatistics statistics = context.getColumnsHistMap().get(columnInfo.getId());

        return 0.01;
      case GREATER_EQUAL:
      case GREATER_THAN:
      case LESS_EQUAL:
      case LESS_THAN:
        return 0.1;
      case NOT_EQUAL:
        return 0.99;
      default:
        return 1.0;
    }
  }


  private List<IndexRange> getMaskAndRanges(List<Expression> exprs, BitSet mask, TiIndexInfo indexInfo, TiTableInfo table) {
    ScanSpec result = ScanAnalyzer.extractConditions(exprs, table, indexInfo);
    List<Expression> pointPredicates = result.getPointPredicates();
    Expression rangePredicate = result.getRangePredicate().orElse(null);
    for (int i = 0; i < exprs.size(); i++) {
      Expression exp = exprs.get(i);
      if (pointPredicates.contains(exp) || rangePredicate == exp) {
        mask.set(i);
      }
    }
    return expressionToIndexRanges(pointPredicates, result.getRangePredicate());
  }



  private class ExprSet {
    int tp;
    long ID;
    BitSet mask;
    List<IndexRange> ranges;

    private ExprSet(int _tp, long _ID, BitSet _mask, List<IndexRange> _ranges) {
      this.tp = _tp;
      this.ID = _ID;
      this.mask = (BitSet) _mask.clone();
      this.ranges = _ranges;
    }

    private String retrieve(int x) {
      switch (x) {
        case 0:
          return "index";
        case 1:
          return "pk";
        case 2:
          return "column";
        default:
          return "";
      }
    }

    @Override
    public String toString() {
      String ans = retrieve(tp) + "#" + String.valueOf(ID) + "_" + mask + "_";
      for (IndexRange ir : ranges) {
        ans = ans.concat("," + ir);
      }
      return ans;
    }
  }

}
