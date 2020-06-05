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

import static com.pingcap.tikv.predicates.PredicateUtils.mergeCNFExpressions;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tikv.exception.TiExpressionException;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class ScanSpec {
  private final List<Expression> pointPredicates;
  private final Optional<Expression> rangePredicate;
  private final Set<Expression> residualPredicates;

  private ScanSpec(
      List<Expression> pointPredicates,
      Optional<Expression> rangePredicate,
      Set<Expression> residualPredicates) {
    this.pointPredicates = pointPredicates;
    this.rangePredicate = rangePredicate;
    this.residualPredicates = residualPredicates;
  }

  public List<Expression> getPointPredicates() {
    return pointPredicates;
  }

  public Optional<Expression> getRangePredicate() {
    return rangePredicate;
  }

  public Set<Expression> getResidualPredicates() {
    return residualPredicates;
  }

  public static class Builder {
    private final IdentityHashMap<TiIndexColumn, List<Expression>> pointPredicates =
        new IdentityHashMap<>();
    private final TiTableInfo table;
    private final TiIndexInfo index;
    private final List<Expression> rangePredicates = new ArrayList<>();
    private final List<Expression> residualPredicates = new ArrayList<>();
    private final Set<Expression> residualCandidates = new HashSet<>();
    private TiIndexColumn rangeColumn;

    public Builder(TiTableInfo table, TiIndexInfo index) {
      this.table = table;
      this.index = index;
    }

    void addResidualPredicate(Expression predicate) {
      residualPredicates.add(predicate);
    }

    void addAllPredicates(List<Expression> predicates) {
      residualCandidates.addAll(predicates);
    }

    void addPointPredicate(TiIndexColumn col, Expression predicate) {
      requireNonNull(col, "index column is null");
      requireNonNull(predicate, "predicate is null");
      if (pointPredicates.containsKey(col)) {
        List<Expression> predicates = pointPredicates.get(col);
        predicates.add(predicate);
      } else {
        List<Expression> predicates = new ArrayList<>();
        predicates.add(predicate);
        pointPredicates.put(col, predicates);
      }
    }

    void addRangePredicate(TiIndexColumn col, Expression predicate) {
      requireNonNull(col, "col is null");
      if (rangeColumn == null) {
        rangeColumn = col;
      } else if (!rangeColumn.equals(col)) {
        throw new TiExpressionException("Cannot reset range predicates");
      }
      rangePredicates.add(predicate);
    }

    public ScanSpec build() {
      List<Expression> points = new ArrayList<>();
      List<DataType> pointTypes = new ArrayList<>();
      Set<Expression> pushedPredicates = new HashSet<>();
      if (index != null) {
        for (TiIndexColumn indexColumn : index.getIndexColumns()) {
          List<Expression> predicates = pointPredicates.get(indexColumn);
          if (predicates == null) {
            break;
          }
          pushedPredicates.addAll(predicates);
          TiColumnInfo tiColumnInfo = table.getColumn(indexColumn.getOffset());
          DataType type = tiColumnInfo.getType();
          points.add(mergeCNFExpressions(predicates));
          pointTypes.add(type);
        }
      }
      Optional<Expression> newRangePred =
          rangePredicates.isEmpty()
              ? Optional.empty()
              : Optional.ofNullable(mergeCNFExpressions(rangePredicates));
      pushedPredicates.addAll(rangePredicates);

      Set<Expression> newResidualPredicates = new HashSet<>(residualPredicates);
      for (Expression pred : residualCandidates) {
        if (!pushedPredicates.contains(pred)) {
          newResidualPredicates.add(pred);
        }
      }

      return new ScanSpec(ImmutableList.copyOf(points), newRangePred, newResidualPredicates);
    }
  }
}
