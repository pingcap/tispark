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
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiExpr;
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
  public static class Builder {
    private final IdentityHashMap<TiIndexColumn, List<TiExpr>> pointPredicates = new IdentityHashMap();
    private TiIndexColumn rangeColumn;
    private final TiTableInfo table;
    private final TiIndexInfo index;
    private final List<TiExpr> rangePredicates = new ArrayList<>();
    private final List<TiExpr> residualPredicates = new ArrayList<>();
    private Set<TiExpr> residualCandidates = new HashSet<>();

    public Builder(TiTableInfo table, TiIndexInfo index) {
      this.table = table;
      this.index = index;
    }

    public void addResidualPredicate(TiExpr predicate) {
      residualPredicates.add(predicate);
    }

    public void addAllPredicates(List<TiExpr> predicates) {
      residualCandidates.addAll(predicates);
    }

    public void addPointPredicate(TiIndexColumn col, TiExpr predicate) {
      requireNonNull(col, "index column is null");
      requireNonNull(predicate, "predicate is null");
      if (pointPredicates.containsKey(col)) {
        List<TiExpr> predicates = pointPredicates.get(col);
        predicates.add(predicate);
      } else {
        List<TiExpr> predicates = new ArrayList<>();
        predicates.add(predicate);
        pointPredicates.put(col, predicates);
      }
    }

    public void addRangePredicate(TiIndexColumn col, TiExpr predicate) {
      requireNonNull(col, "col is null");
      if (!col.equals(rangeColumn)) {
        throw new TiClientInternalException("Cannot reset range predicates");
      }
      this.rangeColumn = col;
      rangePredicates.add(predicate);
    }

    public ScanSpec build() {
      List<TiExpr> points = new ArrayList<>();
      List<DataType> pointTypes = new ArrayList<>();
      Set<TiExpr> pushedPredicates = new HashSet<>();
      for (TiIndexColumn indexColumn : index.getIndexColumns()) {
        List<TiExpr> predicates = pointPredicates.get(indexColumn);
        pushedPredicates.addAll(predicates);
        TiColumnInfo tiColumnInfo = table.getColumn(indexColumn.getOffset());
        DataType type = tiColumnInfo.getType();
        points.add(mergeCNFExpressions(predicates));
        pointTypes.add(type);
      }
      Optional<TiExpr> newRangePred = rangePredicates.isEmpty() ?
          Optional.empty() : Optional.of(mergeCNFExpressions(rangePredicates));
      pushedPredicates.addAll(rangePredicates);

      Set<TiExpr> newResidualPredicates = new HashSet<>(residualPredicates);
      for (TiExpr pred : residualCandidates) {
        if (!pushedPredicates.contains(pred)) {
          newResidualPredicates.add(pred);
        }
      }

      Optional<DataType> rangeType;
      if (rangeColumn == null) {
        rangeType = Optional.empty();
      } else {
        TiColumnInfo col = table.getColumn(rangeColumn.getOffset());
        rangeType = Optional.of(col.getType());
      }

      return new ScanSpec(
          ImmutableList.copyOf(points),
          newRangePred,
          newResidualPredicates);
    }
  }

  private final List<TiExpr> pointPredicates;
  private final Optional<TiExpr> rangePredicate;
  private final Set<TiExpr> residualPredicates;

  private ScanSpec(
      List<TiExpr> pointPredicates,
      Optional<TiExpr> rangePredicate,
      Set<TiExpr> residualPredicates) {
    this.pointPredicates = pointPredicates;
    this.rangePredicate = rangePredicate;
    this.residualPredicates = residualPredicates;
  }

  public List<TiExpr> getPointPredicates() {
    return pointPredicates;
  }

  public Optional<TiExpr> getRangePredicate() {
    return rangePredicate;
  }

  public Set<TiExpr> getResidualPredicates() {
    return residualPredicates;
  }
}
