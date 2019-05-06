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

import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.predicates.PredicateUtils.expressionToIndexRanges;
import static com.pingcap.tikv.util.KeyRangeUtils.makeCoprocRange;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.visitor.IndexMatcher;
import com.pingcap.tikv.expression.visitor.MetaResolver;
import com.pingcap.tikv.expression.visitor.PrunedPartitionBuilder;
import com.pingcap.tikv.key.IndexScanKeyRangeBuilder;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.statistics.IndexStatistics;
import com.pingcap.tikv.statistics.TableStatistics;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class ScanAnalyzer {
  private static final double INDEX_SCAN_COST_FACTOR = 1.2;
  private static final double TABLE_SCAN_COST_FACTOR = 1.0;
  private static final double DOUBLE_READ_COST_FACTOR = TABLE_SCAN_COST_FACTOR * 3;

  public static class ScanPlan {
    ScanPlan(
        List<KeyRange> keyRanges,
        Set<Expression> filters,
        TiIndexInfo index,
        double cost,
        boolean isDoubleRead,
        double estimatedRowCount,
        List<TiPartitionDef> partDefs) {
      this.filters = filters;
      this.keyRanges = keyRanges;
      this.cost = cost;
      this.index = index;
      this.isDoubleRead = isDoubleRead;
      this.estimatedRowCount = estimatedRowCount;
      this.prunedParts = partDefs;
    }

    private final List<KeyRange> keyRanges;
    private final Set<Expression> filters;
    private final double cost;
    private TiIndexInfo index;
    private final boolean isDoubleRead;
    private final double estimatedRowCount;
    private final List<TiPartitionDef> prunedParts;

    public double getEstimatedRowCount() {
      return estimatedRowCount;
    }

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

    public boolean isDoubleRead() {
      return isDoubleRead;
    }

    public List<TiPartitionDef> getPrunedParts() {
      return prunedParts;
    }
  }

  // build a scan for debug purpose.
  public ScanPlan buildScan(
      List<TiColumnInfo> columnList, List<Expression> conditions, TiTableInfo table) {
    return buildScan(columnList, conditions, table, null);
  }

  // Build scan plan picking access path with lowest cost by estimation
  public ScanPlan buildScan(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiTableInfo table,
      TableStatistics tableStatistics) {
    ScanPlan minPlan = buildTableScan(conditions, table, tableStatistics);
    double minCost = minPlan.getCost();
    for (TiIndexInfo index : table.getIndices()) {
      ScanPlan plan = buildIndexScan(columnList, conditions, index, table, tableStatistics);
      if (plan.getCost() < minCost) {
        minPlan = plan;
        minCost = plan.getCost();
      }
    }
    return minPlan;
  }

  public ScanPlan buildTableScan(
      List<Expression> conditions, TiTableInfo table, TableStatistics tableStatistics) {
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    return buildIndexScan(table.getColumns(), conditions, pkIndex, table, tableStatistics);
  }

  ScanPlan buildIndexScan(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiIndexInfo index,
      TiTableInfo table,
      TableStatistics tableStatistics) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(conditions, "conditions cannot be null to encoding keyRange");

    MetaResolver.resolve(conditions, table);

    ScanSpec result = extractConditions(conditions, table, index);

    double cost = SelectivityCalculator.calcPseudoSelectivity(result);

    List<IndexRange> irs =
        expressionToIndexRanges(
            result.getPointPredicates(), result.getRangePredicate(), table, index);

    List<TiPartitionDef> prunedParts = null;
    // apply partition pruning here.
    if (table.getPartitionInfo() != null) {
      PrunedPartitionBuilder prunedPartBuilder = new PrunedPartitionBuilder();
      prunedParts = prunedPartBuilder.prune(table, conditions);
    }

    List<KeyRange> keyRanges;
    boolean isDoubleRead = false;
    double estimatedRowCount = -1;
    // table name and columns
    long tableColSize = table.getColumnLength() + 8;

    if (index == null || index.isFakePrimaryKey()) {
      if (tableStatistics != null) {
        cost = 100.0; // Full table scan cost
        // TODO: Fine-grained statistics usage
      }
      keyRanges = buildTableScanKeyRange(table, irs, prunedParts);
      cost *= tableColSize * TABLE_SCAN_COST_FACTOR;
    } else {
      if (tableStatistics != null) {
        long totalRowCount = tableStatistics.getCount();
        IndexStatistics indexStatistics = tableStatistics.getIndexHistMap().get(index.getId());
        if (conditions.isEmpty()) {
          cost = 100.0; // Full index scan cost
          // TODO: Fine-grained statistics usage
          estimatedRowCount = totalRowCount;
        } else if (indexStatistics != null) {
          double idxRangeRowCnt = indexStatistics.getRowCount(irs);
          // guess the percentage of rows hit
          cost = 100.0 * idxRangeRowCnt / totalRowCount;
          estimatedRowCount = idxRangeRowCnt;
        }
      }
      isDoubleRead = !isCoveringIndex(columnList, index, table.isPkHandle());
      // table name, index and handle column
      long indexSize = index.getIndexColumnLength() + 16;
      if (isDoubleRead) {
        cost *= tableColSize * DOUBLE_READ_COST_FACTOR + indexSize * INDEX_SCAN_COST_FACTOR;
      } else {
        cost *= indexSize * INDEX_SCAN_COST_FACTOR;
      }
      keyRanges = buildIndexScanKeyRange(table, index, irs, prunedParts);
    }

    return new ScanPlan(
        keyRanges,
        result.getResidualPredicates(),
        index,
        cost,
        isDoubleRead,
        estimatedRowCount,
        prunedParts);
  }

  private Pair<Key, Key> buildTableScanKeyRangePerId(long id, IndexRange ir) {
    Key startKey;
    Key endKey;
    if (ir.hasAccessKey()) {
      checkArgument(
          !ir.hasRange(), "Table scan must have one and only one access condition / point");

      Key key = ir.getAccessKey();
      checkArgument(key instanceof TypedKey, "Table scan key range must be typed key");
      TypedKey typedKey = (TypedKey) key;
      startKey = RowKey.toRowKey(id, typedKey);
      endKey = startKey.next();
    } else if (ir.hasRange()) {
      checkArgument(
          !ir.hasAccessKey(), "Table scan must have one and only one access condition / point");
      Range<TypedKey> r = ir.getRange();

      if (!r.hasLowerBound()) {
        // -INF
        startKey = RowKey.createMin(id);
      } else {
        // Comparison with null should be filtered since it yields unknown always
        startKey = RowKey.toRowKey(id, r.lowerEndpoint());
        if (r.lowerBoundType().equals(BoundType.OPEN)) {
          startKey = startKey.next();
        }
      }

      if (!r.hasUpperBound()) {
        // INF
        endKey = RowKey.createBeyondMax(id);
      } else {
        endKey = RowKey.toRowKey(id, r.upperEndpoint());
        if (r.upperBoundType().equals(BoundType.CLOSED)) {
          endKey = endKey.next();
        }
      }
    } else {
      throw new TiClientInternalException("Empty access conditions");
    }
    return new Pair<>(startKey, endKey);
  }

  private List<KeyRange> buildTableScanKeyRangeWithIds(
      List<Long> ids, List<IndexRange> indexRanges) {
    List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
    for (Long id : ids) {
      indexRanges.forEach(
          (ir) -> {
            Pair<Key, Key> pairKey = buildTableScanKeyRangePerId(id, ir);
            Key startKey = pairKey.first;
            Key endKey = pairKey.second;
            // This range only possible when < MIN or > MAX
            if (!startKey.equals(endKey)) {
              ranges.add(makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
            }
          });
    }
    return ranges;
  }

  @VisibleForTesting
  List<KeyRange> buildTableScanKeyRange(
      TiTableInfo table, List<IndexRange> indexRanges, List<TiPartitionDef> prunedParts) {
    requireNonNull(table, "Table is null");
    requireNonNull(indexRanges, "indexRanges is null");

    if (table.isPartitionEnabled()) {
      List<Long> ids = new ArrayList<>();
      for (TiPartitionDef pDef : prunedParts) {
        ids.add(pDef.getId());
      }
      return buildTableScanKeyRangeWithIds(ids, indexRanges);
    } else {
      return buildTableScanKeyRangeWithIds(ImmutableList.of(table.getId()), indexRanges);
    }
  }

  @VisibleForTesting
  List<KeyRange> buildIndexScanKeyRange(
      TiTableInfo table,
      TiIndexInfo index,
      List<IndexRange> indexRanges,
      List<TiPartitionDef> prunedParts) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(index, "Index cannot be null to encoding keyRange");
    requireNonNull(indexRanges, "indexRanges cannot be null to encoding keyRange");

    List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
    for (IndexRange ir : indexRanges) {
      if (!table.isPartitionEnabled()) {
        IndexScanKeyRangeBuilder indexScanKeyRangeBuilder =
            new IndexScanKeyRangeBuilder(table.getId(), index, ir);
        ranges.add(indexScanKeyRangeBuilder.compute());
      } else {
        for (TiPartitionDef pDef : prunedParts) {
          IndexScanKeyRangeBuilder indexScanKeyRangeBuilder =
              new IndexScanKeyRangeBuilder(pDef.getId(), index, ir);
          ranges.add(indexScanKeyRangeBuilder.compute());
        }
      }
    }

    return ranges;
  }

  boolean isCoveringIndex(
      List<TiColumnInfo> columns, TiIndexInfo indexColumns, boolean pkIsHandle) {
    for (TiColumnInfo colInfo : columns) {
      if (pkIsHandle && colInfo.isPrimaryKey()) {
        continue;
      }
      if (colInfo.getId() == -1) {
        continue;
      }
      boolean isIndexColumn = false;
      for (TiIndexColumn indexCol : indexColumns.getIndexColumns()) {
        boolean isFullLength =
            indexCol.getLength() == DataType.UNSPECIFIED_LEN
                || indexCol.getLength() == colInfo.getType().getLength();
        if (colInfo.getName().equalsIgnoreCase(indexCol.getName()) && isFullLength) {
          isIndexColumn = true;
          break;
        }
      }
      if (!isIndexColumn) {
        return false;
      }
    }
    return true;
  }

  @VisibleForTesting
  public static ScanSpec extractConditions(
      List<Expression> conditions, TiTableInfo table, TiIndexInfo index) {
    // 0. Different than TiDB implementation, here logic has been unified for TableScan and
    // IndexScan by
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
