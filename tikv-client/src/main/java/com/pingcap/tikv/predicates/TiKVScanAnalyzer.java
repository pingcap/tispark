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
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class TiKVScanAnalyzer {
  private static final double INDEX_SCAN_COST_FACTOR = 1.2;
  private static final double TABLE_SCAN_COST_FACTOR = 1.0;
  private static final double DOUBLE_READ_COST_FACTOR = TABLE_SCAN_COST_FACTOR * 3;

  public static class TiKVScanPlan {
    public static class Builder {
      private Map<Long, List<KeyRange>> keyRanges;
      private Set<Expression> filters;
      private double cost;
      private TiIndexInfo index;
      private boolean isDoubleRead;
      private double estimatedRowCount = -1;
      private List<TiPartitionDef> prunedParts;

      private Builder() {}

      public static Builder newBuilder() {
        return new Builder();
      }

      public void setKeyRanges(Map<Long, List<KeyRange>> keyRanges) {
        this.keyRanges = keyRanges;
      }

      public void setFilters(Set<Expression> filters) {
        this.filters = filters;
      }

      public void setCost(double cost) {
        this.cost = cost;
      }

      public void setIndex(TiIndexInfo index) {
        this.index = index;
      }

      public void setDoubleRead(boolean doubleRead) {
        isDoubleRead = doubleRead;
      }

      public void setEstimatedRowCount(double estimatedRowCount) {
        this.estimatedRowCount = estimatedRowCount;
      }

      public void setPrunedParts(List<TiPartitionDef> prunedParts) {
        this.prunedParts = prunedParts;
      }

      public TiKVScanPlan build() {
        return new TiKVScanPlan(
            keyRanges, filters, index, cost, isDoubleRead, estimatedRowCount, prunedParts);
      }

      // TODO: Fine-grained statistics usage
      Builder calculateCostAndEstimateCount(TableStatistics tableStatistics, long tableColSize) {
        cost = 100.0;
        cost *= tableColSize * TABLE_SCAN_COST_FACTOR;
        return this;
      }

      Builder calculateCostAndEstimateCount(
          TableStatistics tableStatistics,
          List<Expression> conditions,
          List<IndexRange> irs,
          long indexSize,
          long tableColSize) {
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

          if (isDoubleRead) {
            cost *= tableColSize * DOUBLE_READ_COST_FACTOR + indexSize * INDEX_SCAN_COST_FACTOR;
          } else {
            cost *= indexSize * INDEX_SCAN_COST_FACTOR;
          }
        }
        return this;
      }
    }

    TiKVScanPlan(
        Map<Long, List<KeyRange>> keyRanges,
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

    private final Map<Long, List<KeyRange>> keyRanges;
    private final Set<Expression> filters;
    private final double cost;
    private TiIndexInfo index;
    private final boolean isDoubleRead;
    private final double estimatedRowCount;
    private final List<TiPartitionDef> prunedParts;

    public double getEstimatedRowCount() {
      return estimatedRowCount;
    }

    public Map<Long, List<KeyRange>> getKeyRanges() {
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
  public TiKVScanPlan buildScan(
      List<TiColumnInfo> columnList, List<Expression> conditions, TiTableInfo table) {
    return buildScan(columnList, conditions, table, null);
  }

  // Build scan plan picking access path with lowest cost by estimation
  public TiKVScanPlan buildScan(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiTableInfo table,
      TableStatistics tableStatistics) {
    TiKVScanPlan minPlan = buildTableScan(conditions, table, tableStatistics);
    double minCost = minPlan.getCost();
    for (TiIndexInfo index : table.getIndices()) {
      TiKVScanPlan plan = buildIndexScan(columnList, conditions, index, table, tableStatistics);
      if (plan.getCost() < minCost) {
        minPlan = plan;
        minCost = plan.getCost();
      }
    }
    return minPlan;
  }

  public TiKVScanPlan buildTableScan(
      List<Expression> conditions, TiTableInfo table, TableStatistics tableStatistics) {
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    return buildIndexScan(table.getColumns(), conditions, pkIndex, table, tableStatistics);
  }

  public TiKVScanPlan buildIndexScan(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiIndexInfo index,
      TiTableInfo table,
      TableStatistics tableStatistics) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(conditions, "conditions cannot be null to encoding keyRange");

    MetaResolver.resolve(conditions, table);

    TiKVScanPlan.Builder planBuilder = TiKVScanPlan.Builder.newBuilder();
    ScanSpec result = extractConditions(conditions, table, index);

    double cost = SelectivityCalculator.calcPseudoSelectivity(result);
    planBuilder.setCost(cost);

    List<IndexRange> irs =
        expressionToIndexRanges(
            result.getPointPredicates(), result.getRangePredicate(), table, index);

    List<TiPartitionDef> prunedParts = null;
    // apply partition pruning here.
    if (table.getPartitionInfo() != null) {
      PrunedPartitionBuilder prunedPartBuilder = new PrunedPartitionBuilder();
      prunedParts = prunedPartBuilder.prune(table, conditions);
    }

    // table name and columns
    long tableColSize = table.getColumnLength() + 8;

    if (index == null || index.isFakePrimaryKey()) {
      planBuilder.setDoubleRead(false);
      planBuilder.calculateCostAndEstimateCount(tableStatistics, tableColSize);
      planBuilder.setKeyRanges(buildTableScanKeyRange(table, irs, prunedParts));
    } else {
      planBuilder.setIndex(index);
      planBuilder.setDoubleRead(!isCoveringIndex(columnList, index, table.isPkHandle()));
      // table name, index and handle column
      long indexSize = index.getIndexColumnLength() + 16;
      planBuilder.calculateCostAndEstimateCount(
          tableStatistics, conditions, irs, indexSize, tableColSize);
      planBuilder.setKeyRanges(buildIndexScanKeyRange(table, index, irs, prunedParts));
    }

    planBuilder.setFilters(result.getResidualPredicates());
    planBuilder.setPrunedParts(prunedParts);
    return planBuilder.build();
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

  private Map<Long, List<KeyRange>> buildTableScanKeyRangeWithIds(
      List<Long> ids, List<IndexRange> indexRanges) {
    Map<Long, List<KeyRange>> idRanges = new HashMap<>(ids.size());
    for (Long id : ids) {
      List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
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

      idRanges.put(id, ranges);
    }
    return idRanges;
  }

  @VisibleForTesting
  public Map<Long, List<KeyRange>> buildTableScanKeyRange(
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
  private Map<Long, List<KeyRange>> buildIndexScanKeyRangeWithIds(
      List<Long> ids, TiIndexInfo index, List<IndexRange> indexRanges) {
    Map<Long, List<KeyRange>> idRanes = new HashMap<>();
    for (long id : ids) {
      List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
      for (IndexRange ir : indexRanges) {
        IndexScanKeyRangeBuilder indexScanKeyRangeBuilder =
            new IndexScanKeyRangeBuilder(id, index, ir);
        ranges.add(indexScanKeyRangeBuilder.compute());
      }

      idRanes.put(id, ranges);
    }
    return idRanes;
  }

  @VisibleForTesting
  Map<Long, List<KeyRange>> buildIndexScanKeyRange(
      TiTableInfo table,
      TiIndexInfo index,
      List<IndexRange> indexRanges,
      List<TiPartitionDef> prunedParts) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(index, "Index cannot be null to encoding keyRange");
    requireNonNull(indexRanges, "indexRanges cannot be null to encoding keyRange");

    if (table.isPartitionEnabled()) {
      List<Long> ids = new ArrayList<>();
      for (TiPartitionDef pDef : prunedParts) {
        ids.add(pDef.getId());
      }
      return buildIndexScanKeyRangeWithIds(ids, index, indexRanges);
    } else {
      return buildIndexScanKeyRangeWithIds(ImmutableList.of(table.getId()), index, indexRanges);
    }
  }

  // If all the columns requested in the select list of query, are available in the index, then the
  // query engine doesn't have to lookup the table again compared with double read.
  boolean isCoveringIndex(
      List<TiColumnInfo> columns, TiIndexInfo indexColumns, boolean pkIsHandle) {
    Map<String, TiIndexColumn> colInIndex =
        indexColumns
            .getIndexColumns()
            .stream()
            .collect(Collectors.toMap(TiIndexColumn::getName, col -> col));
    for (TiColumnInfo colInfo : columns) {
      if (pkIsHandle && colInfo.isPrimaryKey()) {
        continue;
      }
      if (colInfo.getId() == -1) {
        continue;
      }
      boolean colNotInIndex = false;
      if (colInIndex.containsKey(colInfo.getName())) {
        TiIndexColumn indexCol = colInIndex.get(colInfo.getName());
        boolean isFullLength =
            indexCol.isLengthUnspecified() || indexCol.getLength() == colInfo.getType().getLength();
        if (!colInfo.getName().equalsIgnoreCase(indexCol.getName()) || !isFullLength) {
          colNotInIndex = true;
        }
      } else {
        colNotInIndex = true;
      }
      if (colNotInIndex) {
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
