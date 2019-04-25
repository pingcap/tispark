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

package com.pingcap.tikv.meta;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.pingcap.tikv.predicates.PredicateUtils.mergeCNFExpressions;
import static java.util.Objects.requireNonNull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.*;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.DAGRequestException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.ByItem;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.visitor.ExpressionTypeCoercer;
import com.pingcap.tikv.expression.visitor.MetaResolver;
import com.pingcap.tikv.expression.visitor.ProtoConverter;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.Pair;
import java.io.*;
import java.util.*;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.tikv.kvproto.Coprocessor;

/**
 * Type TiDAGRequest.
 *
 * <p>Used for constructing a new DAG request to TiKV
 */
public class TiDAGRequest implements Serializable {

  public List<TiPartitionDef> getPrunedParts() {
    return prunedParts;
  }

  public void setPrunedParts(List<TiPartitionDef> prunedParts) {
    this.prunedParts = prunedParts;
  }

  public static class Builder {
    private List<String> requiredCols = new ArrayList<>();
    private List<Expression> filters = new ArrayList<>();
    private List<ByItem> orderBys = new ArrayList<>();
    private List<Coprocessor.KeyRange> ranges = new ArrayList<>();
    private TiTableInfo tableInfo;
    private int limit;
    private TiTimestamp startTs;

    public static Builder newBuilder() {
      return new Builder();
    }

    public Builder setFullTableScan(TiTableInfo tableInfo) {
      requireNonNull(tableInfo);
      setTableInfo(tableInfo);
      if (!tableInfo.isPartitionEnabled()) {
        RowKey start = RowKey.createMin(tableInfo.getId());
        RowKey end = RowKey.createBeyondMax(tableInfo.getId());
        ranges.add(KeyRangeUtils.makeCoprocRange(start.toByteString(), end.toByteString()));
      } else {
        for (TiPartitionDef pDef : tableInfo.getPartitionInfo().getDefs()) {
          RowKey start = RowKey.createMin(pDef.getId());
          RowKey end = RowKey.createBeyondMax(pDef.getId());
          ranges.add(KeyRangeUtils.makeCoprocRange(start.toByteString(), end.toByteString()));
        }
      }

      return this;
    }

    public Builder setLimit(int limit) {
      this.limit = limit;
      return this;
    }

    public Builder setTableInfo(TiTableInfo tableInfo) {
      this.tableInfo = tableInfo;
      return this;
    }

    public Builder addRequiredCols(String... cols) {
      this.requiredCols.addAll(Arrays.asList(cols));
      return this;
    }

    public Builder addRequiredCols(List<String> cols) {
      this.requiredCols.addAll(cols);
      return this;
    }

    public Builder addFilter(Expression filter) {
      this.filters.add(filter);
      return this;
    }

    public Builder addOrderBy(ByItem item) {
      this.orderBys.add(item);
      return this;
    }

    public Builder setStartTs(@Nonnull TiTimestamp ts) {
      this.startTs = ts;
      return this;
    }

    public TiDAGRequest build(PushDownType pushDownType) {
      TiDAGRequest req = new TiDAGRequest(pushDownType);
      req.setTableInfo(tableInfo);
      req.addRanges(ranges);
      filters.forEach(req::addFilter);
      if (!orderBys.isEmpty()) {
        orderBys.forEach(req::addOrderByItem);
      }
      if (limit != 0) {
        req.setLimit(limit);
      }
      requiredCols.forEach(c -> req.addRequiredColumn(ColumnRef.create(c)));
      req.setStartTs(startTs);

      req.resolve();
      return req;
    }
  }

  public TiDAGRequest(PushDownType pushDownType) {
    this.pushDownType = pushDownType;
  }

  public TiDAGRequest(PushDownType pushDownType, int timeZoneOffset) {
    this(pushDownType);
    this.timeZoneOffset = timeZoneOffset;
  }

  public enum TruncateMode {
    IgnoreTruncation(0x1),
    TruncationAsWarning(0x2);

    private final long mask;

    TruncateMode(long mask) {
      this.mask = mask;
    }

    public long mask(long flags) {
      return flags | mask;
    }
  }

  /** Whether we use streaming to push down the request */
  public enum PushDownType {
    STREAMING,
    NORMAL
  }

  /** Predefined executor priority map. */
  private static final Map<ExecType, Integer> EXEC_TYPE_PRIORITY_MAP =
      ImmutableMap.<ExecType, Integer>builder()
          .put(ExecType.TypeTableScan, 0)
          .put(ExecType.TypeIndexScan, 0)
          .put(ExecType.TypeSelection, 1)
          .put(ExecType.TypeAggregation, 2)
          .put(ExecType.TypeTopN, 3)
          .put(ExecType.TypeLimit, 4)
          .build();

  private TiTableInfo tableInfo;
  private List<TiPartitionDef> prunedParts;
  private TiIndexInfo indexInfo;
  private final List<ColumnRef> fields = new ArrayList<>();
  private final List<Expression> filters = new ArrayList<>();
  private final List<ByItem> groupByItems = new ArrayList<>();
  private final List<ByItem> orderByItems = new ArrayList<>();
  private List<Expression> residualFilters = null;
  // System like Spark has different type promotion rules
  // we need a cast to target when given
  private final List<Pair<Expression, DataType>> aggregates = new ArrayList<>();
  private final List<Coprocessor.KeyRange> keyRanges = new ArrayList<>();
  // If index scanning of this request is not possible in some scenario, we downgrade it to a table
  // scan and use
  // downGradeRanges instead of index scan ranges stored in keyRanges along with downgradeFilters to
  // perform a
  // table scan.
  private List<Expression> downgradeFilters = new ArrayList<>();

  private int limit;
  private int timeZoneOffset;
  private long flags;
  private TiTimestamp startTs;
  private Expression having;
  private boolean distinct;
  private boolean isDoubleRead;
  private final PushDownType pushDownType;
  private IdentityHashMap<Expression, DataType> typeMap;
  private double estimatedCount = -1;

  private static ColumnInfo handleColumn =
      ColumnInfo.newBuilder()
          .setColumnId(-1)
          .setPkHandle(true)
          // We haven't changed the field name in protobuf file, but
          // we need to set this to true in order to retrieve the handle,
          // so the name 'setPkHandle' may sounds strange.
          .build();

  private List<Expression> getAllExpressions() {
    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
    builder.addAll(getFields());
    builder.addAll(getFilters());
    builder.addAll(getAggregates());
    getGroupByItems().forEach(item -> builder.add(item.getExpr()));
    getOrderByItems().forEach(item -> builder.add(item.getExpr()));
    if (having != null) {
      builder.add(having);
    }
    return builder.build();
  }

  public DataType getExpressionType(Expression expression) {
    requireNonNull(typeMap, "request is not resolved");
    return typeMap.get(expression);
  }

  public void resolve() {
    MetaResolver resolver = new MetaResolver(tableInfo);
    ExpressionTypeCoercer inferrer = new ExpressionTypeCoercer();
    resolver.resolve(getAllExpressions());
    inferrer.infer(getAllExpressions());
    typeMap = inferrer.getTypeMap();
  }

  /**
   * Unify indexScan and tableScan building logic since they are very much alike. DAGRequest for
   * IndexScan should also contain filters and aggregation, so we can reuse this part of logic.
   *
   * <p>DAGRequest is made up of a chain of executors with strict orders: TableScan/IndexScan >
   * Selection > Aggregation > TopN/Limit a DAGRequest must contain one and only one TableScan or
   * IndexScan.
   *
   * @param buildIndexScan whether the dagRequest to build should be an {@link
   *     com.pingcap.tidb.tipb.IndexScan}
   * @return final DAGRequest built
   */
  public DAGRequest buildScan(boolean buildIndexScan) {
    long id = tableInfo.getId();
    checkNotNull(startTs, "startTs is null");
    checkArgument(startTs.getVersion() != 0, "timestamp is 0");
    DAGRequest.Builder dagRequestBuilder = DAGRequest.newBuilder();
    Executor.Builder executorBuilder = Executor.newBuilder();
    IndexScan.Builder indexScanBuilder = IndexScan.newBuilder();
    TableScan.Builder tblScanBuilder = TableScan.newBuilder();
    // find a column's offset in fields
    Map<ColumnRef, Integer> colOffsetInFieldMap = new HashMap<>();
    // find a column's position in index
    Map<TiColumnInfo, Integer> colPosInIndexMap = new HashMap<>();

    if (buildIndexScan) {
      // IndexScan
      if (indexInfo == null) {
        throw new TiClientInternalException("Index is empty for index scan");
      }
      List<TiColumnInfo> columnInfoList = tableInfo.getColumns();
      boolean hasPk = false;
      // We extract index column info
      List<Integer> indexColOffsets =
          indexInfo
              .getIndexColumns()
              .stream()
              .map(TiIndexColumn::getOffset)
              .collect(Collectors.toList());

      int idxPos = 0;
      // for index scan builder, columns are added by its order in index
      for (Integer idx : indexColOffsets) {
        TiColumnInfo tiColumnInfo = columnInfoList.get(idx);
        ColumnInfo columnInfo = tiColumnInfo.toProto(tableInfo);
        colPosInIndexMap.put(tiColumnInfo, idxPos++);

        ColumnInfo.Builder colBuilder = ColumnInfo.newBuilder(columnInfo);
        if (columnInfo.getColumnId() == -1) {
          hasPk = true;
          colBuilder.setPkHandle(true);
        }
        indexScanBuilder.addColumns(colBuilder);
      }

      if (isDoubleRead()) {
        // double read case
        if (!hasPk) {
          indexScanBuilder.addColumns(handleColumn);
        }

        int colCount = indexScanBuilder.getColumnsCount();
        // double read case: need to retrieve handle
        dagRequestBuilder.addOutputOffsets(colCount != 0 ? colCount - 1 : 0);
      } else {
        int colCount = indexScanBuilder.getColumnsCount();
        boolean pkIsNeeded = false;
        // =================== IMPORTANT ======================
        // offset for dagRequest should be in accordance with fields
        for (ColumnRef col : getFields()) {
          Integer pos = colPosInIndexMap.get(col.getColumnInfo());
          if (pos != null) {
            TiColumnInfo columnInfo = columnInfoList.get(indexColOffsets.get(pos));
            if (col.getColumnInfo().equals(columnInfo)) {
              dagRequestBuilder.addOutputOffsets(pos);
              colOffsetInFieldMap.put(col, pos);
            }
          }
          // if a column of field is not contained in index selected,
          // logically it must be the pk column and
          // the pkIsHandle must be true. Extra check here.
          else if (col.getColumnInfo().isPrimaryKey() && tableInfo.isPkHandle()) {
            pkIsNeeded = true;
            // offset should be processed for each primary key encountered
            dagRequestBuilder.addOutputOffsets(colCount);
            // for index scan, column offset must be in the order of index->handle
            colOffsetInFieldMap.put(col, indexColOffsets.size());
          } else {
            throw new DAGRequestException(
                "columns other than primary key and index key exist in fields while index single read: "
                    + col.getName());
          }
        }
        // pk is not included in index but still needed
        if (pkIsNeeded) {
          indexScanBuilder.addColumns(handleColumn);
        }
      }
      executorBuilder.setTp(ExecType.TypeIndexScan);

      indexScanBuilder.setTableId(id).setIndexId(indexInfo.getId());
      dagRequestBuilder.addExecutors(executorBuilder.setIdxScan(indexScanBuilder).build());
    } else {
      // TableScan
      executorBuilder.setTp(ExecType.TypeTableScan);
      tblScanBuilder.setTableId(id);
      // Step1. Add columns to first executor
      int lastOffset = 0;
      for (ColumnRef col : getFields()) {
        // can't allow duplicated col added into executor.
        if (!colOffsetInFieldMap.containsKey(col)) {
          tblScanBuilder.addColumns(col.getColumnInfo().toProto(tableInfo));
          colOffsetInFieldMap.put(col, lastOffset);
          lastOffset++;
        }
        // column offset should be in accordance with fields
        dagRequestBuilder.addOutputOffsets(colOffsetInFieldMap.get(col));
      }

      dagRequestBuilder.addExecutors(executorBuilder.setTblScan(tblScanBuilder));

      // clear executorBuilder
      executorBuilder.clear();

      // Step2. Add others
      // DO NOT EDIT EXPRESSION CONSTRUCTION ORDER
      // Or make sure the construction order is below:
      // TableScan/IndexScan > Selection > Aggregation > TopN/Limit
      Expression whereExpr = mergeCNFExpressions(getFilters());
      if (whereExpr != null) {
        executorBuilder.setTp(ExecType.TypeSelection);
        dagRequestBuilder.addExecutors(
            executorBuilder.setSelection(
                Selection.newBuilder()
                    .addConditions(ProtoConverter.toProto(whereExpr, colOffsetInFieldMap))));
        executorBuilder.clear();
      }

      if (!getGroupByItems().isEmpty() || !getAggregates().isEmpty()) {
        Aggregation.Builder aggregationBuilder = Aggregation.newBuilder();
        getGroupByItems()
            .forEach(
                tiByItem ->
                    aggregationBuilder.addGroupBy(
                        ProtoConverter.toProto(tiByItem.getExpr(), colOffsetInFieldMap)));
        getAggregates()
            .forEach(
                tiExpr ->
                    aggregationBuilder.addAggFunc(
                        ProtoConverter.toProto(tiExpr, colOffsetInFieldMap)));
        executorBuilder.setTp(ExecType.TypeAggregation);
        dagRequestBuilder.addExecutors(executorBuilder.setAggregation(aggregationBuilder));
        executorBuilder.clear();
      }

      if (!getOrderByItems().isEmpty()) {
        TopN.Builder topNBuilder = TopN.newBuilder();
        getOrderByItems()
            .forEach(
                tiByItem ->
                    topNBuilder.addOrderBy(
                        com.pingcap.tidb.tipb.ByItem.newBuilder()
                            .setExpr(
                                ProtoConverter.toProto(tiByItem.getExpr(), colOffsetInFieldMap))
                            .setDesc(tiByItem.isDesc())));
        executorBuilder.setTp(ExecType.TypeTopN);
        topNBuilder.setLimit(getLimit());
        dagRequestBuilder.addExecutors(executorBuilder.setTopN(topNBuilder));
        executorBuilder.clear();
      } else if (getLimit() != 0) {
        Limit.Builder limitBuilder = Limit.newBuilder();
        limitBuilder.setLimit(getLimit());
        executorBuilder.setTp(ExecType.TypeLimit);
        dagRequestBuilder.addExecutors(executorBuilder.setLimit(limitBuilder));
        executorBuilder.clear();
      }
    }

    DAGRequest request =
        dagRequestBuilder
            .setTimeZoneOffset(timeZoneOffset)
            .setFlags(flags)
            .setStartTs(startTs.getVersion())
            .build();

    validateRequest(request);
    return request;
  }

  /**
   * Check if a DAG request is valid.
   *
   * <p>Note: When constructing a DAG request, a executor with an ExecType of higher priority should
   * always be placed before those lower ones.
   *
   * @param dagRequest Request DAG.
   */
  private void validateRequest(DAGRequest dagRequest) {
    requireNonNull(dagRequest);
    // A DAG request must has at least one executor.
    if (dagRequest.getExecutorsCount() < 1) {
      throw new DAGRequestException("Invalid executors count:" + dagRequest.getExecutorsCount());
    }

    ExecType formerType = dagRequest.getExecutors(0).getTp();
    if (formerType != ExecType.TypeTableScan && formerType != ExecType.TypeIndexScan) {
      throw new DAGRequestException(
          "Invalid first executor type:"
              + formerType
              + ", must one of TypeTableScan or TypeIndexScan");
    }

    for (int i = 1; i < dagRequest.getExecutorsCount(); i++) {
      ExecType currentType = dagRequest.getExecutors(i).getTp();
      if (EXEC_TYPE_PRIORITY_MAP.get(currentType) < EXEC_TYPE_PRIORITY_MAP.get(formerType)) {
        throw new DAGRequestException("Invalid executor priority.");
      }
      formerType = currentType;
    }
  }

  public TiDAGRequest setTableInfo(TiTableInfo tableInfo) {
    this.tableInfo = requireNonNull(tableInfo, "tableInfo is null");
    return this;
  }

  public TiTableInfo getTableInfo() {
    return this.tableInfo;
  }

  public List<Long> getIds() {
    if (!this.tableInfo.isPartitionEnabled()) {
      return ImmutableList.of(this.tableInfo.getId());
    }

    List<Long> ids = new ArrayList<>();
    for (TiPartitionDef pDef : this.getPrunedParts()) {
      ids.add(pDef.getId());
    }
    return ids;
  }

  public TiDAGRequest setIndexInfo(TiIndexInfo indexInfo) {
    this.indexInfo = requireNonNull(indexInfo, "indexInfo is null");
    return this;
  }

  public TiIndexInfo getIndexInfo() {
    return indexInfo;
  }

  public void clearIndexInfo() {
    indexInfo = null;
  }

  public int getLimit() {
    return limit;
  }

  /**
   * add limit clause to select query.
   *
   * @param limit is just a integer.
   * @return a SelectBuilder
   */
  public TiDAGRequest setLimit(int limit) {
    this.limit = limit;
    return this;
  }

  /**
   * set timezone offset
   *
   * @param timeZoneOffset timezone offset
   * @return a TiDAGRequest
   */
  public TiDAGRequest setTimeZoneOffset(int timeZoneOffset) {
    this.timeZoneOffset = timeZoneOffset;
    return this;
  }

  int getTimeZoneOffset() {
    return timeZoneOffset;
  }

  /**
   * set truncate mode
   *
   * @param mode truncate mode
   * @return a TiDAGRequest
   */
  public TiDAGRequest setTruncateMode(TiDAGRequest.TruncateMode mode) {
    flags = requireNonNull(mode, "mode is null").mask(flags);
    return this;
  }

  @VisibleForTesting
  long getFlags() {
    return flags;
  }

  /**
   * set start timestamp for the transaction
   *
   * @param startTs timestamp
   * @return a TiDAGRequest
   */
  public TiDAGRequest setStartTs(@Nonnull TiTimestamp startTs) {
    this.startTs = startTs;
    return this;
  }

  @VisibleForTesting
  public TiTimestamp getStartTs() {
    return startTs;
  }

  /**
   * set having clause to select query
   *
   * @param having is a expression represents Having
   * @return a TiDAGRequest
   */
  public TiDAGRequest setHaving(Expression having) {
    this.having = requireNonNull(having, "having is null");
    return this;
  }

  public TiDAGRequest setDistinct(boolean distinct) {
    this.distinct = distinct;
    return this;
  }

  public boolean isDistinct() {
    return distinct;
  }

  public TiDAGRequest addAggregate(Expression expr, DataType targetType) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(Pair.create(expr, targetType));
    return this;
  }

  public List<Expression> getAggregates() {
    return aggregates.stream().map(p -> p.first).collect(Collectors.toList());
  }

  public List<Pair<Expression, DataType>> getAggregatePairs() {
    return aggregates;
  }

  /**
   * add a order by clause to select query.
   *
   * @param byItem is a TiByItem.
   * @return a SelectBuilder
   */
  public TiDAGRequest addOrderByItem(ByItem byItem) {
    orderByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  List<ByItem> getOrderByItems() {
    return orderByItems;
  }

  /**
   * add a group by clause to select query
   *
   * @param byItem is a TiByItem
   * @return a SelectBuilder
   */
  public TiDAGRequest addGroupByItem(ByItem byItem) {
    groupByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  public List<ByItem> getGroupByItems() {
    return groupByItems;
  }

  /**
   * Field is not support in TiDB yet, for here we simply allow TiColumnRef instead of TiExpr like
   * in SelectRequest proto
   *
   * <p>
   *
   * <p>This interface allows duplicate columns and it's user's responsibility to do dedup since we
   * need to ensure exact order and items preserved during decoding
   *
   * @param column is column referred during selectReq
   */
  public TiDAGRequest addRequiredColumn(ColumnRef column) {
    fields.add(requireNonNull(column, "columnRef is null"));
    return this;
  }

  public List<ColumnRef> getFields() {
    return fields;
  }

  /**
   * set key range of scan
   *
   * @param ranges key range of scan
   */
  public TiDAGRequest addRanges(List<Coprocessor.KeyRange> ranges) {
    keyRanges.addAll(requireNonNull(ranges, "KeyRange is null"));
    return this;
  }

  public void resetFilters(List<Expression> filters) {
    this.filters.clear();
    this.filters.addAll(filters);
  }

  public List<Coprocessor.KeyRange> getRanges() {
    return keyRanges;
  }

  public TiDAGRequest addFilter(Expression filter) {
    this.filters.add(requireNonNull(filter, "filters expr is null"));
    return this;
  }

  public List<Expression> getDowngradeFilters() {
    return downgradeFilters;
  }

  public TiDAGRequest addDowngradeFilter(Expression filter) {
    this.downgradeFilters.add(requireNonNull(filter, "downgrade filter is null"));
    return this;
  }

  /**
   * Check whether the DAG request has any aggregate expression.
   *
   * @return the boolean
   */
  public boolean hasAggregate() {
    return !getAggregates().isEmpty();
  }

  /**
   * Check whether the DAG request has any group by expression.
   *
   * @return the boolean
   */
  public boolean hasGroupBy() {
    return !getGroupByItems().isEmpty();
  }

  public List<Expression> getFilters() {
    return filters;
  }

  /**
   * Returns whether needs to read handle from index first and find its corresponding row. i.e,
   * "double read"
   *
   * @return boolean
   */
  public boolean isDoubleRead() {
    return isDoubleRead;
  }

  /**
   * Sets isDoubleRead
   *
   * @param isDoubleRead if is double read
   */
  public void setIsDoubleRead(boolean isDoubleRead) {
    this.isDoubleRead = isDoubleRead;
  }

  /**
   * Returns whether the request is CoveringIndex
   *
   * @return boolean
   */
  public boolean isCoveringIndexScan() {
    return hasIndex() && !isDoubleRead();
  }

  /**
   * Returns whether this request is of indexScanType
   *
   * @return true iff indexInfo is provided, false otherwise
   */
  public boolean hasIndex() {
    return indexInfo != null;
  }

  /**
   * Whether we use streaming processing to retrieve data
   *
   * @return push down type.
   */
  public PushDownType getPushDownType() {
    return pushDownType;
  }

  /** Set the estimated row count will be fetched from this request. */
  public void setEstimatedCount(double estimatedCount) {
    this.estimatedCount = estimatedCount;
  }

  /** Get the estimated row count will be fetched from this request. */
  public double getEstimatedCount() {
    return estimatedCount;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (tableInfo != null) {
      sb.append(String.format("[table: %s] ", tableInfo.getName()));
    }

    if (indexInfo != null) {
      sb.append(String.format("[Index: %s] ", indexInfo.getName()));
    }

    if (!getFields().isEmpty()) {
      sb.append(", Columns: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getFields());
    }

    if (!getDowngradeFilters().isEmpty()) {
      // should be called after all parameters are set
      if (residualFilters == null) {
        residualFilters = new ArrayList<>(getDowngradeFilters());
        residualFilters.removeAll(new HashSet<>(getFilters()));
      }
      if (!residualFilters.isEmpty()) {
        sb.append(", Residual Filter: ");
        Joiner.on(", ").skipNulls().appendTo(sb, residualFilters);
      }
    }

    if (!getFilters().isEmpty()) {
      sb.append(", PushDown Filter: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getFilters());
    }

    // Key ranges might be also useful
    if (!getRanges().isEmpty()) {
      sb.append(", KeyRange: ");
      getRanges().forEach(x -> sb.append(KeyUtils.formatBytes(x)));
    }

    if (!getAggregates().isEmpty()) {
      sb.append(", Aggregates: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getAggregates());
    }

    if (!getGroupByItems().isEmpty()) {
      sb.append(", Group By: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getGroupByItems());
    }

    if (!getOrderByItems().isEmpty()) {
      sb.append(", Order By: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getOrderByItems());
    }

    if (getLimit() != 0) {
      sb.append(", Limit: ");
      sb.append("[").append(limit).append("]");
    }
    return sb.toString();
  }

  public TiDAGRequest copy() {
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(this);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      ObjectInputStream ois = new ObjectInputStream(bais);
      return ((TiDAGRequest) ois.readObject());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
