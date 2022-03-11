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
import com.pingcap.tidb.tipb.EncodeType;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.PartitionPruner;
import com.pingcap.tikv.expression.visitor.IndexMatcher;
import com.pingcap.tikv.key.IndexScanKeyRangeBuilder;
import com.pingcap.tikv.key.Key;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.key.TypedKey;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiDAGRequest;
import com.pingcap.tikv.meta.TiDAGRequest.IndexScanType;
import com.pingcap.tikv.meta.TiIndexColumn;
import com.pingcap.tikv.meta.TiIndexInfo;
import com.pingcap.tikv.meta.TiPartitionDef;
import com.pingcap.tikv.meta.TiTableInfo;
import com.pingcap.tikv.meta.TiTimestamp;
import com.pingcap.tikv.region.TiStoreType;
import com.pingcap.tikv.statistics.IndexStatistics;
import com.pingcap.tikv.statistics.TableStatistics;
import com.pingcap.tikv.types.MySQLType;
import com.pingcap.tikv.util.Pair;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Coprocessor.KeyRange;

public class TiKVScanAnalyzer {
  private static final double INDEX_SCAN_COST_FACTOR = 1.2;
  private static final double TABLE_SCAN_COST_FACTOR = 1.0;
  private static final double DOUBLE_READ_COST_FACTOR = TABLE_SCAN_COST_FACTOR * 3;
  private static final long TABLE_PREFIX_SIZE = 8;
  private static final long INDEX_PREFIX_SIZE = 8;

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

  // build a scan for debug purpose.
  public TiDAGRequest buildTiDAGReq(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiTableInfo table,
      TiTimestamp ts,
      TiDAGRequest dagRequest) {
    return buildTiDAGReq(
        true, false, true, false, columnList, conditions, table, null, ts, dagRequest);
  }

  // Build scan plan picking access path with lowest cost by estimation
  public TiDAGRequest buildTiDAGReq(
      boolean allowIndexScan,
      boolean useIndexScanFirst,
      boolean canUseTiKV,
      boolean canUseTiFlash,
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiTableInfo table,
      TableStatistics tableStatistics,
      TiTimestamp ts,
      TiDAGRequest dagRequest) {

    TiKVScanPlan minPlan = null;
    if (canUseTiKV) {
      minPlan = buildTableScan(conditions, table, tableStatistics);
    }
    if (canUseTiFlash) {
      // it is possible that only TiFlash plan exists due to isolation read.
      TiKVScanPlan plan = buildTiFlashScan(columnList, conditions, table, tableStatistics);
      if (minPlan == null || plan.getCost() < minPlan.getCost()) {
        minPlan = plan;
      }
    } else if (canUseTiKV && allowIndexScan) {
      minPlan.getFilters().forEach(dagRequest::addDowngradeFilter);
      if (table.isPartitionEnabled()) {
        // disable index scan
      } else {
        TiKVScanPlan minIndexPlan = null;
        double minIndexCost = Double.MAX_VALUE;
        for (TiIndexInfo index : table.getIndices()) {
          if (table.isCommonHandle() && table.getPrimaryKey().equals(index)) {
            continue;
          }

          if (supportIndexScan(index, table)) {
            TiKVScanPlan plan =
                buildIndexScan(columnList, conditions, index, table, tableStatistics, false);
            if (plan.getCost() < minIndexCost) {
              minIndexPlan = plan;
              minIndexCost = plan.getCost();
            }
          }
        }
        if (minIndexPlan != null && (minIndexCost < minPlan.getCost() || useIndexScanFirst)) {
          minPlan = minIndexPlan;
        }
      }
    }
    if (minPlan == null) {
      throw new TiClientInternalException(
          "No valid plan found for table '" + table.getName() + "'");
    }

    TiStoreType minPlanStoreType = minPlan.getStoreType();
    // TiKV should not use CHBlock as Encode Type.
    if (minPlanStoreType == TiStoreType.TiKV
        && dagRequest.getEncodeType() == EncodeType.TypeCHBlock) {
      dagRequest.setEncodeType(EncodeType.TypeChunk);
    }
    // Set DAG Request's store type as minPlan's store type.
    dagRequest.setStoreType(minPlanStoreType);

    dagRequest.addRanges(minPlan.getKeyRanges());
    dagRequest.setPrunedParts(minPlan.getPrunedParts());
    dagRequest.addFilters(new ArrayList<>(minPlan.getFilters()));
    if (minPlan.isIndexScan()) {
      dagRequest.setIndexInfo(minPlan.getIndex());
      // need to set isDoubleRead to true for dagRequest in case of double read
      dagRequest.setIsDoubleRead(minPlan.isDoubleRead());
    }

    dagRequest.setTableInfo(table);
    dagRequest.setStartTs(ts);
    dagRequest.setEstimatedCount(minPlan.getEstimatedRowCount());
    return dagRequest;
  }

  private TiKVScanPlan buildTableScan(
      List<Expression> conditions, TiTableInfo table, TableStatistics tableStatistics) {
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    return buildIndexScan(table.getColumns(), conditions, pkIndex, table, tableStatistics, false);
  }

  private TiKVScanPlan buildTiFlashScan(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiTableInfo table,
      TableStatistics tableStatistics) {
    TiIndexInfo pkIndex = TiIndexInfo.generateFakePrimaryKeyIndex(table);
    return buildIndexScan(columnList, conditions, pkIndex, table, tableStatistics, true);
  }

  TiKVScanPlan buildIndexScan(
      List<TiColumnInfo> columnList,
      List<Expression> conditions,
      TiIndexInfo index,
      TiTableInfo table,
      TableStatistics tableStatistics,
      boolean useTiFlash) {
    requireNonNull(table, "Table cannot be null to encoding keyRange");
    requireNonNull(conditions, "conditions cannot be null to encoding keyRange");

    TiKVScanPlan.Builder planBuilder = TiKVScanPlan.Builder.newBuilder(table.getName());
    ScanSpec result = extractConditions(conditions, table, index);

    double cost = SelectivityCalculator.calcPseudoSelectivity(result);
    planBuilder.setCost(cost);

    List<IndexRange> irs =
        expressionToIndexRanges(
            result.getPointPredicates(), result.getRangePredicate(), table, index);

    List<TiPartitionDef> prunedParts = null;
    // apply partition pruning here.
    if (table.getPartitionInfo() != null) {
      prunedParts = PartitionPruner.prune(table, conditions);
    }
    planBuilder.setFilters(result.getResidualPredicates()).setPrunedParts(prunedParts);

    // table name and columns
    long tableColSize = table.getEstimatedRowSizeInByte() + TABLE_PREFIX_SIZE;

    if (index == null || index.isFakePrimaryKey()) {
      planBuilder
          .setDoubleRead(false)
          .setKeyRanges(buildTableScanKeyRange(table, irs, prunedParts));
      if (useTiFlash) {
        // TiFlash is a columnar storage engine
        long colSize =
            columnList.stream().mapToLong(TiColumnInfo::getSize).sum() + TABLE_PREFIX_SIZE;
        return planBuilder
            .setStoreType(TiStoreType.TiFlash)
            .calculateCostAndEstimateCount(tableStatistics, colSize)
            .build();
      } else {
        return planBuilder.calculateCostAndEstimateCount(tableStatistics, tableColSize).build();
      }
    } else {
      // TiFlash does not support index scan.
      assert (!useTiFlash);
      long indexSize = index.getIndexColumnSize() + TABLE_PREFIX_SIZE + INDEX_PREFIX_SIZE;
      return planBuilder
          .setIndex(index)
          .setDoubleRead(!isCoveringIndex(columnList, index, table.isPkHandle()))
          // table name, index and handle column
          .calculateCostAndEstimateCount(tableStatistics, conditions, irs, indexSize, tableColSize)
          .setKeyRanges(buildIndexScanKeyRange(table, index, irs, prunedParts))
          .build();
    }
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
  Map<Long, List<KeyRange>> buildTableScanKeyRange(
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
    Map<Long, List<KeyRange>> idRanges = new HashMap<>();
    for (long id : ids) {
      List<KeyRange> ranges = new ArrayList<>(indexRanges.size());
      for (IndexRange ir : indexRanges) {
        IndexScanKeyRangeBuilder indexScanKeyRangeBuilder =
            new IndexScanKeyRangeBuilder(id, index, ir);
        ranges.add(indexScanKeyRangeBuilder.compute());
      }

      idRanges.put(id, ranges);
    }
    return idRanges;
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
    if (columns.isEmpty()) {
      return false;
    }

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

  public static class TiKVScanPlan {
    private final Map<Long, List<KeyRange>> keyRanges;
    private final Set<Expression> filters;
    private final double cost;
    private final TiIndexInfo index;
    private final boolean isDoubleRead;
    private final double estimatedRowCount;
    private final List<TiPartitionDef> prunedParts;
    private final TiStoreType storeType;

    private TiKVScanPlan(
        Map<Long, List<KeyRange>> keyRanges,
        Set<Expression> filters,
        TiIndexInfo index,
        double cost,
        boolean isDoubleRead,
        double estimatedRowCount,
        List<TiPartitionDef> partDefs,
        TiStoreType storeType) {
      this.filters = filters;
      this.keyRanges = keyRanges;
      this.cost = cost;
      this.index = index;
      this.isDoubleRead = isDoubleRead;
      this.estimatedRowCount = estimatedRowCount;
      this.prunedParts = partDefs;
      this.storeType = storeType;
    }

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

    public TiStoreType getStoreType() {
      return storeType;
    }

    public static class Builder {
      private final String tableName;
      private final Logger logger = LoggerFactory.getLogger(getClass().getName());
      private Map<Long, List<KeyRange>> keyRanges;
      private Set<Expression> filters;
      private double cost;
      private TiIndexInfo index;
      private boolean isDoubleRead;
      private double estimatedRowCount = -1;
      private List<TiPartitionDef> prunedParts;
      private TiStoreType storeType = TiStoreType.TiKV;

      private Builder(String tableName) {
        this.tableName = tableName;
      }

      public static Builder newBuilder(String tableName) {
        return new Builder(tableName);
      }

      public Builder setKeyRanges(Map<Long, List<KeyRange>> keyRanges) {
        this.keyRanges = keyRanges;
        return this;
      }

      public Builder setFilters(Set<Expression> filters) {
        this.filters = filters;
        return this;
      }

      public Builder setCost(double cost) {
        this.cost = cost;
        return this;
      }

      public Builder setIndex(TiIndexInfo index) {
        this.index = index;
        return this;
      }

      public Builder setDoubleRead(boolean doubleRead) {
        isDoubleRead = doubleRead;
        return this;
      }

      public Builder setEstimatedRowCount(double estimatedRowCount) {
        this.estimatedRowCount = estimatedRowCount;
        return this;
      }

      public Builder setPrunedParts(List<TiPartitionDef> prunedParts) {
        this.prunedParts = prunedParts;
        return this;
      }

      public Builder setStoreType(TiStoreType storeType) {
        this.storeType = storeType;
        return this;
      }

      public TiKVScanPlan build() {
        return new TiKVScanPlan(
            keyRanges,
            filters,
            index,
            cost,
            isDoubleRead,
            estimatedRowCount,
            prunedParts,
            storeType);
      }

      private void debug(IndexScanType scanType) {
        String plan, desc;
        switch (scanType) {
          case TABLE_SCAN:
            plan = "TableScan";
            desc = storeType.toString();
            break;
          case INDEX_SCAN:
            plan = "IndexScan";
            desc = index.getName();
            break;
          case COVERING_INDEX_SCAN:
            plan = "CoveringIndexScan";
            desc = index.getName();
            break;
          default:
            // should not reach
            plan = "None";
            desc = "";
        }
        logger.debug(
            "[Table:"
                + tableName
                + "]["
                + plan
                + ":"
                + desc
                + "] cost="
                + cost
                + " estimated row count="
                + estimatedRowCount);
      }

      // TODO: Fine-grained statistics usage
      Builder calculateCostAndEstimateCount(TableStatistics tableStatistics, long tableColSize) {
        cost = 100.0;
        cost *= tableColSize * TABLE_SCAN_COST_FACTOR;
        if (tableStatistics != null) {
          estimatedRowCount = tableStatistics.getCount();
        }
        debug(IndexScanType.TABLE_SCAN);
        return this;
      }

      Builder calculateCostAndEstimateCount(
          TableStatistics tableStatistics,
          List<Expression> conditions,
          List<IndexRange> irs,
          long indexSize,
          long tableColSize) {
        if (tableStatistics != null) {
          double totalRowCount = tableStatistics.getCount();
          IndexStatistics indexStatistics = tableStatistics.getIndexHistMap().get(index.getId());
          if (indexStatistics != null) {
            totalRowCount = indexStatistics.getHistogram().totalRowCount();
          }
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
            debug(IndexScanType.INDEX_SCAN);
          } else {
            cost *= indexSize * INDEX_SCAN_COST_FACTOR;
            debug(IndexScanType.COVERING_INDEX_SCAN);
          }
        }
        return this;
      }
    }
  }

  private boolean supportIndexScan(TiIndexInfo index, TiTableInfo table) {
    // YEAR TYPE index scan is disabled, https://github.com/pingcap/tispark/issues/1789
    for (TiIndexColumn tiIndexColumn : index.getIndexColumns()) {
      TiColumnInfo tiColumnInfo = table.getColumn(tiIndexColumn.getName());
      if (tiColumnInfo.getType().getType() == MySQLType.TypeYear) {
        return false;
      }
    }
    return true;
  }
}
