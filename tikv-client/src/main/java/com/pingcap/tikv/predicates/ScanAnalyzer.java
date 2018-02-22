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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.visitor.IndexMatcher;
import com.pingcap.tikv.expression.visitor.MetaResolver;
import com.pingcap.tikv.key.IndexKey;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.kvproto.Coprocessor.KeyRange;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.statistics.ColumnStatistics;
import com.pingcap.tikv.statistics.TableStatistics;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.predicates.PredicateUtils.expressionToIndexRanges;
import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;
import static java.util.Objects.requireNonNull;


public class ScanAnalyzer {
  public static class ScanPlan {
    public ScanPlan(List<KeyRange> keyRanges, Set<Expression> filters, TiIndexInfo index, double cost) {
      this.filters = filters;
      this.keyRanges = keyRanges;
      this.cost = cost;
      this.index = index;
    }

    private final List<KeyRange> keyRanges;
    private final Set<Expression> filters;
    private final double cost;
    private TiIndexInfo index;

    public List<KeyRange> getKeyRanges() {
      return keyRanges;
    }

    public Set<Expression> getFilters() {
      return filters;
    }

    public double getCost() {
      return cost;
    }

    public boolean isIndexScan() {
      return index != null && !index.isFakePrimaryKey();
    }

    public TiIndexInfo getIndex() {
      return index;
    }
  }

  // Build scan plan picking access path with lowest cost by estimation
  public ScanPlan buildScan(List<Expression> conditions, TiTableInfo table) {
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    ScanPlan minPlan = buildScan(conditions, pkIndex, table);
    double minCost = minPlan.getCost();
    for (TiIndexInfo index : table.getIndices()) {
      ScanPlan plan = buildScan(conditions, index, table);
      if (plan.getCost() < minCost) {
        minPlan = plan;
        minCost = plan.getCost();
      }
    }
    return minPlan;
  }

  public ScanPlan buildTableScan(List<Expression> conditions, TiTableInfo table, TableStatistics ts) {
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    ScanPlan plan = buildScan(conditions, pkIndex, table);
//    ColumnStatistics cs = ts.getColumnsHistMap().get(table.getId());
//    ScanSpec result = extractConditions(conditions, table, pkIndex);
//    List<IndexRange> irs = expressionToIndexRanges(result.getPointPredicates(), result.getRangePredicate());
//    double cnt = cs.getColumnRowCount(irs);
//    System.out.println(cnt);
    return plan;
  }

  public ScanPlan buildScan(List<Expression> conditions, TiIndexInfo index, TiTableInfo table) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(conditions, "conditions cannot be null to encoding keyRange");

    MetaResolver.resolve(conditions, table);

    ScanSpec result = extractConditions(conditions, table, index);
    double cost = SelectivityCalculator.calcPseudoSelectivity(result);

    List<IndexRange> irs = expressionToIndexRanges(result.getPointPredicates(), result.getRangePredicate());

    List<KeyRange> keyRanges;
    if (index == null || index.isFakePrimaryKey()) {
      keyRanges = buildTableScanKeyRange(table, irs);
    } else {
      keyRanges = buildIndexScanKeyRange(table, index, irs);
    }

    return new ScanPlan(keyRanges, result.getResidualPredicates(), index, cost);
  }

  @VisibleForTesting
  List<KeyRange> buildTableScanKeyRange(TiTableInfo table, List<IndexRange> indexRanges) {
    requireNonNull(table, "Table is null");
    requireNonNull(indexRanges, "indexRanges is null");

    List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
    for (IndexRange ir : indexRanges) {
      Key startKey;
      Key endKey;
      if (ir.hasAccessKey()) {
        checkArgument(!ir.hasRange(), "Table scan must have one and only one access condition / point");

        Key key = ir.getAccessKey();
        checkArgument(key instanceof TypedKey, "Table scan key range must be typed key");
        TypedKey typedKey = (TypedKey) key;
        startKey = RowKey.toRowKey(table.getId(), typedKey);
        endKey = startKey.next();
      } else if (ir.hasRange()) {
        checkArgument(!ir.hasAccessKey(), "Table scan must have one and only one access condition / point");
        Range<TypedKey> r = ir.getRange();

        if (!r.hasLowerBound()) {
          // -INF
          startKey = RowKey.createMin(table.getId());
        } else {
          // Comparision with null should be filtered since it yields unknown always
          startKey = RowKey.toRowKey(table.getId(), r.lowerEndpoint());
          if (r.lowerBoundType().equals(BoundType.OPEN)) {
            startKey = startKey.next();
          }
        }

        if (!r.hasUpperBound()) {
          // INF
          endKey = RowKey.createBeyondMax(table.getId());
        } else {
          endKey = RowKey.toRowKey(table.getId(), r.upperEndpoint());
          if (r.upperBoundType().equals(BoundType.CLOSED)) {
            endKey = endKey.next();
          }
        }
      } else {
        throw new TiClientInternalException("Empty access conditions");
      }

      ranges.add(makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
    }

    if (ranges.isEmpty()) {
      Key startKey = RowKey.createMin(table.getId());
      Key endKey = RowKey.createBeyondMax(table.getId());
      ranges.add(makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
    }

    return ranges;
  }

  @VisibleForTesting
  List<KeyRange> buildIndexScanKeyRange(
      TiTableInfo table, TiIndexInfo index, List<IndexRange> indexRanges) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(index, "Index cannot be null to encoding keyRange");
    requireNonNull(index, "indexRanges cannot be null to encoding keyRange");

    List<KeyRange> ranges = new ArrayList<>(indexRanges.size());

    for (IndexRange ir : indexRanges) {
      Key pointKey = ir.hasAccessKey() ? ir.getAccessKey() : Key.EMPTY;

      Range<TypedKey> range = ir.getRange();
      Key lPointKey;
      Key uPointKey;

      Key lKey;
      Key uKey;
      if (!ir.hasRange()) {
        lPointKey = pointKey;
        uPointKey = pointKey.next();

        lKey = Key.EMPTY;
        uKey = Key.EMPTY;
      } else {
        lPointKey = pointKey;
        uPointKey = pointKey;

        if (!range.hasLowerBound()) {
          // -INF
          lKey = Key.MIN;
        } else {
          lKey = range.lowerEndpoint();
          if (range.lowerBoundType().equals(BoundType.OPEN)) {
            lKey = lKey.next();
          }
        }

        if (!range.hasUpperBound()) {
          // INF
          uKey = Key.MAX;
        } else {
          uKey = range.upperEndpoint();
          if (range.upperBoundType().equals(BoundType.CLOSED)) {
            uKey = uKey.next();
          }
        }
      }
      IndexKey lbsKey = IndexKey.toIndexKey(table.getId(), index.getId(), lPointKey, lKey);
      IndexKey ubsKey = IndexKey.toIndexKey(table.getId(), index.getId(), uPointKey, uKey);

      ranges.add(makeCoprocRange(lbsKey.toByteString(), ubsKey.toByteString()));
    }

    if (ranges.isEmpty()) {
      ranges.add(makeCoprocRange(Key.MIN.toByteString(), Key.MAX.toByteString()));
    }
    return ranges;
  }

  @VisibleForTesting
  static ScanSpec extractConditions(
      List<Expression> conditions, TiTableInfo table, TiIndexInfo index) {
    // 0. Different than TiDB implementation, here logic has been unified for TableScan and IndexScan by
    // adding fake index on clustered table's pk
    // 1. Generate access point based on equal conditions
    // 2. Cut access point condition if index is not continuous
    // 3. Push back prefix index conditions since prefix index retrieve more result than needed
    // 4. For remaining indexes (since access conditions consume some index, and they will
    // not be used in filter push down later), find continuous matching index until first unmatched
    // 5. Push back index related filter if prefix index, for remaining filters
    // Equal conditions needs to be process first according to index sequence
    // When index is null, no access condition can be applied
    ScanSpec.Builder specBuilder = new ScanSpec.Builder(table, index);
    if (index != null) {
      Set<Expression> visited = new HashSet<>();
      IndexMatchingLoop:
      for (int i = 0; i < index.getIndexColumns().size(); i++) {
        // for each index column try matches an equal condition
        // and push remaining back
        // TODO: if more than one equal conditions match an
        // index, it likely yields nothing. Maybe a check needed
        // to simplify it to a false condition
        TiIndexColumn col = index.getIndexColumns().get(i);
        IndexMatcher eqMatcher = IndexMatcher.equalOnlyMatcher(col);
        boolean found = false;
        // For first prefix index encountered, it equals to a range
        // and we cannot push equal conditions further
        for (Expression cond : conditions) {
          if (visited.contains(cond)) {
            continue;
          }
          if (eqMatcher.match(cond)) {
            specBuilder.addPointPredicate(col, cond);
            if (col.isPrefixIndex()) {
              specBuilder.addResidualPredicate(cond);
              break IndexMatchingLoop;
            }
            visited.add(cond);
            found = true;
            break;
          }
        }
        if (!found) {
          // For first "broken index chain piece"
          // search for a matching range condition
          IndexMatcher matcher = IndexMatcher.matcher(col);
          for (Expression cond : conditions) {
            if (visited.contains(cond)) {
              continue;
            }
            if (matcher.match(cond)) {
              specBuilder.addRangePredicate(col, cond);
              if (col.isPrefixIndex()) {
                specBuilder.addResidualPredicate(cond);
                break;
              }
            }
          }
          break;
        }
      }
    }

    specBuilder.addAllPredicates(conditions);
    return specBuilder.build();
  }
}
