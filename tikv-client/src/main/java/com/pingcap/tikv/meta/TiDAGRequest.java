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
import com.pingcap.tidb.tipb.Aggregation;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tidb.tipb.DAGRequest;
import com.pingcap.tidb.tipb.EncodeType;
import com.pingcap.tidb.tipb.ExecType;
import com.pingcap.tidb.tipb.Executor;
import com.pingcap.tidb.tipb.IndexScan;
import com.pingcap.tidb.tipb.Limit;
import com.pingcap.tidb.tipb.Selection;
import com.pingcap.tidb.tipb.TableScan;
import com.pingcap.tidb.tipb.TopN;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.DAGRequestException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.AggregateFunction;
import com.pingcap.tikv.expression.ByItem;
import com.pingcap.tikv.expression.ColumnRef;
import com.pingcap.tikv.expression.Expression;
import com.pingcap.tikv.expression.visitor.ProtoConverter;
import com.pingcap.tikv.key.RowKey;
import com.pingcap.tikv.predicates.PredicateUtils;
import com.pingcap.tikv.region.TiStoreType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.util.KeyRangeUtils;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.tikv.kvproto.Coprocessor;

/**
 * Type TiDAGRequest.
 *
 * <p>Used for constructing a new DAG request to TiKV
 */
public class TiDAGRequest implements Serializable {
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

  private static final ColumnInfo handleColumn =
      ColumnInfo.newBuilder()
          .setColumnId(-1)
          .setPkHandle(true)
          // We haven't changed the field name in protobuf file, but
          // we need to set this to true in order to retrieve the handle,
          // so the name 'setPkHandle' may sounds strange.
          .setTp(8)
          .setColumnLen(20)
          .setFlag(2)
          .build();
  private final List<ColumnRef> fields = new ArrayList<>();
  private final List<DataType> indexDataTypes = new ArrayList<>();
  private final List<Expression> filters = new ArrayList<>();
  private final List<ByItem> groupByItems = new ArrayList<>();
  private final List<ByItem> orderByItems = new ArrayList<>();
  // System like Spark has different type promotion rules
  // we need a cast to target when given
  private final List<AggregateFunction> aggregates = new ArrayList<>();
  private final Map<Long, List<Coprocessor.KeyRange>> idToRanges = new HashMap<>();
  // If index scanning of this request is not possible in some scenario, we downgrade it
  // to a table scan and use downGradeRanges instead of index scan ranges stored in
  // idToRanges along with downgradeFilters to perform a table scan.
  private final List<Expression> downgradeFilters = new ArrayList<>();
  private final List<Expression> pushDownFilters = new ArrayList<>();
  private final List<AggregateFunction> pushDownAggregates = new ArrayList<>();
  private final List<ByItem> pushDownGroupBys = new ArrayList<>();
  private final List<ByItem> pushDownOrderBys = new ArrayList<>();
  private final PushDownType pushDownType;
  private TiTableInfo tableInfo;
  private List<TiPartitionDef> prunedParts;
  private TiStoreType storeType = TiStoreType.TiKV;
  private TiIndexInfo indexInfo;
  private List<Long> prunedPhysicalIds = new ArrayList<>();
  private final Map<Long, String> prunedPartNames = new HashMap<>();
  private long physicalId;
  private int pushDownLimits;
  private int limit;
  private int timeZoneOffset;
  private long flags;
  private TiTimestamp startTs;
  private Expression having;
  private boolean distinct;
  private boolean isDoubleRead;
  private EncodeType encodeType;
  private double estimatedCount = -1;

  public TiDAGRequest(PushDownType pushDownType) {
    this.pushDownType = pushDownType;
    this.encodeType = EncodeType.TypeDefault;
  }

  private TiDAGRequest(PushDownType pushDownType, EncodeType encodeType) {
    this.pushDownType = pushDownType;
    this.encodeType = encodeType;
  }

  public TiDAGRequest(PushDownType pushDownType, EncodeType encodeType, int timeZoneOffset) {
    this(pushDownType, encodeType);
    this.timeZoneOffset = timeZoneOffset;
  }

  public TiDAGRequest(PushDownType pushDownType, int timeZoneOffset) {
    this(pushDownType, EncodeType.TypeDefault);
    this.timeZoneOffset = timeZoneOffset;
  }

  public List<TiPartitionDef> getPrunedParts() {
    return prunedParts;
  }

  private String getPrunedPartName(long id) {
    return prunedPartNames.getOrDefault(id, "unknown");
  }

  public void setPrunedParts(List<TiPartitionDef> prunedParts) {
    this.prunedParts = prunedParts;
    if (prunedParts != null) {
      List<Long> ids = new ArrayList<>();
      prunedPartNames.clear();
      for (TiPartitionDef pDef : prunedParts) {
        ids.add(pDef.getId());
        prunedPartNames.put(pDef.getId(), pDef.getName());
      }
      this.prunedPhysicalIds = ids;
    }
  }

  public List<Long> getPrunedPhysicalIds() {
    if (!this.tableInfo.isPartitionEnabled()) {
      return prunedPhysicalIds = ImmutableList.of(this.tableInfo.getId());
    } else {
      return prunedPhysicalIds;
    }
  }

  public TiStoreType getStoreType() {
    return storeType;
  }

  public void setStoreType(TiStoreType storeType) {
    this.storeType = storeType;
  }

  public EncodeType getEncodeType() {
    return encodeType;
  }

  public void setEncodeType(EncodeType encodeType) {
    this.encodeType = encodeType;
  }

  public boolean isCommonHandle() {
    return tableInfo.isCommonHandle();
  }

  public DAGRequest buildIndexScan() {
    List<Integer> outputOffsets = new ArrayList<>();
    DAGRequest.Builder builder = buildScan(true, outputOffsets);
    return buildRequest(builder, outputOffsets);
  }

  public DAGRequest buildTableScan() {
    List<Integer> outputOffsets = new ArrayList<>();
    boolean isCoveringIndex = isCoveringIndexScan();
    DAGRequest.Builder builder = buildScan(isCoveringIndex, outputOffsets);
    return buildRequest(builder, outputOffsets);
  }

  private DAGRequest buildRequest(
      DAGRequest.Builder dagRequestBuilder, List<Integer> outputOffsets) {
    checkNotNull(startTs, "startTs is null");
    checkArgument(startTs.getVersion() != 0, "timestamp is 0");
    DAGRequest request =
        dagRequestBuilder
            .setTimeZoneOffset(timeZoneOffset)
            .setFlags(flags)
            .addAllOutputOffsets(outputOffsets)
            .setEncodeType(this.encodeType)
            // set start ts fallback is to solving compatible issue.
            .setStartTsFallback(startTs.getVersion())
            .build();

    validateRequest(request);
    return request;
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
  private DAGRequest.Builder buildScan(boolean buildIndexScan, List<Integer> outputOffsets) {
    long id = getPhysicalId();
    checkNotNull(startTs, "startTs is null");
    checkArgument(startTs.getVersion() != 0, "timestamp is 0");
    clearPushDownInfo();
    DAGRequest.Builder dagRequestBuilder = DAGRequest.newBuilder();
    Executor.Builder executorBuilder = Executor.newBuilder();
    IndexScan.Builder indexScanBuilder = IndexScan.newBuilder();
    TableScan.Builder tblScanBuilder = TableScan.newBuilder();
    // find a column's offset in fields
    Map<String, Integer> colOffsetInFieldMap = new HashMap<>();
    // find a column's position in index
    Map<String, Integer> colPosInIndexMap = new HashMap<>();

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
        colPosInIndexMap.put(tiColumnInfo.getName(), idxPos++);

        ColumnInfo.Builder colBuilder = ColumnInfo.newBuilder(columnInfo);
        if (columnInfo.getColumnId() == -1) {
          hasPk = true;
          colBuilder.setPkHandle(true);
        }
        indexScanBuilder.addColumns(colBuilder);
      }

      int colCount = indexScanBuilder.getColumnsCount();
      if (isDoubleRead()) {
        // double read case: need to retrieve handle
        // =================== IMPORTANT ======================
        // offset for dagRequest should be in accordance with fields
        // The last pos will be the handle
        // TODO: we may merge indexDoubleRead and coveringIndexRead logic
        for (ColumnRef col : getFields()) {
          Integer pos = colPosInIndexMap.get(col.getName());
          if (pos != null) {
            TiColumnInfo columnInfo = columnInfoList.get(indexColOffsets.get(pos));
            if (col.matchName(columnInfo.getName())) {
              colOffsetInFieldMap.put(col.getName(), pos);
            }
            // TODO: primary key may also be considered if pkIsHandle
          }
        }
        // double read case
        if (!hasPk) {
          // add handle column
          if (!tableInfo.isCommonHandle()) {
            indexScanBuilder.addColumns(handleColumn);
            ++colCount;
          } else {
            for (TiIndexColumn col : tableInfo.getPrimaryKey().getIndexColumns()) {
              indexScanBuilder.addColumns(tableInfo.getColumn(col.getName()).toProto(tableInfo));
              ++colCount;
            }
          }
          addRequiredIndexDataType();
        }

        if (colCount == 0) {
          throw new DAGRequestException("Incorrect index scan with zero column count");
        }

        if (!tableInfo.isCommonHandle()) {
          outputOffsets.add(colCount - 1);
        } else {
          int idxColSize = tableInfo.getPrimaryKey().getIndexColumns().size();
          for (int i = idxColSize; i >= 1; i--) {
            outputOffsets.add(colCount - i);
          }
        }
      } else {
        boolean pkIsNeeded = false;
        // =================== IMPORTANT ======================
        // offset for dagRequest should be in accordance with fields
        for (ColumnRef col : getFields()) {
          Integer pos = colPosInIndexMap.get(col.getName());
          if (pos != null) {
            TiColumnInfo columnInfo = columnInfoList.get(indexColOffsets.get(pos));
            if (col.matchName(columnInfo.getName())) {
              outputOffsets.add(pos);
              colOffsetInFieldMap.put(col.getName(), pos);
            }
          }
          // if a column of field is not contained in index selected,
          // logically it must be the pk column. Extra check here.
          else if (tableInfo.getColumn(col.getName()).isPrimaryKey()) {
            pkIsNeeded = true;
            // offset should be processed for each primary key encountered
            outputOffsets.add(colCount);
            // for index scan, column offset must be in the order of index->handle
            colOffsetInFieldMap.put(col.getName(), indexColOffsets.size());
          } else {
            throw new DAGRequestException(
                "columns other than primary key and index key exist in fields while index single read: "
                    + col.getName());
          }
        }
        // pk is not included in index but still needed
        if (pkIsNeeded) {
          if (!tableInfo.isCommonHandle()) {
            indexScanBuilder.addColumns(handleColumn);
          }
        }
      }
      executorBuilder.setTp(ExecType.TypeIndexScan);

      indexScanBuilder.setTableId(id).setIndexId(indexInfo.getId());

      if (tableInfo.isCommonHandle()) {
        for (TiIndexColumn col : tableInfo.getPrimaryKey().getIndexColumns()) {
          indexScanBuilder.addPrimaryColumnIds(tableInfo.getColumn(col.getName()).getId());
        }
      }

      dagRequestBuilder.addExecutors(executorBuilder.setIdxScan(indexScanBuilder).build());
    } else {
      // TableScan
      executorBuilder.setTp(ExecType.TypeTableScan);
      tblScanBuilder.setTableId(id);

      if (tableInfo.isCommonHandle()) {
        for (TiIndexColumn col : tableInfo.getPrimaryKey().getIndexColumns()) {
          tblScanBuilder.addPrimaryColumnIds(tableInfo.getColumn(col.getName()).getId());
        }
      }

      // Step1. Add columns to first executor
      int lastOffset = 0;
      for (ColumnRef col : getFields()) {
        // can't allow duplicated col added into executor.
        if (!colOffsetInFieldMap.containsKey(col.getName())) {
          tblScanBuilder.addColumns(tableInfo.getColumn(col.getName()).toProto(tableInfo));
          colOffsetInFieldMap.put(col.getName(), lastOffset);
          lastOffset++;
        }
        // column offset should be in accordance with fields
        outputOffsets.add(colOffsetInFieldMap.get(col.getName()));
      }

      dagRequestBuilder.addExecutors(executorBuilder.setTblScan(tblScanBuilder));
    }

    boolean isIndexDoubleScan = buildIndexScan && isDoubleRead();

    // Should build these executors when performing CoveringIndexScan/TableScan

    // clear executorBuilder
    executorBuilder.clear();

    // Step2. Add others
    // DO NOT EDIT EXPRESSION CONSTRUCTION ORDER
    // Or make sure the construction order is below:
    // TableScan/IndexScan > Selection > Aggregation > TopN/Limit

    Expression whereExpr = mergeCNFExpressions(getFilters());
    if (whereExpr != null) {
      if (!isIndexDoubleScan || isExpressionCoveredByIndex(whereExpr)) {
        executorBuilder.setTp(ExecType.TypeSelection);
        dagRequestBuilder.addExecutors(
            executorBuilder.setSelection(
                Selection.newBuilder()
                    .addConditions(ProtoConverter.toProto(whereExpr, colOffsetInFieldMap))));
        executorBuilder.clear();
        addPushDownFilters();
      } else {
        return dagRequestBuilder;
      }
    }

    if (!getGroupByItems().isEmpty() || !getAggregates().isEmpty()) {
      // only allow table scan or covering index scan push down groupby and agg
      if (!isIndexDoubleScan || (isGroupByCoveredByIndex() && isAggregateCoveredByIndex())) {
        pushDownAggAndGroupBy(
            dagRequestBuilder, executorBuilder, outputOffsets, colOffsetInFieldMap);
      } else {
        return dagRequestBuilder;
      }
    }

    if (!getOrderByItems().isEmpty()) {
      if (!isIndexDoubleScan || isOrderByCoveredByIndex()) {
        // only allow table scan or covering index scan push down orderby
        pushDownOrderBy(dagRequestBuilder, executorBuilder, colOffsetInFieldMap);
      }
    } else if (getLimit() != 0) {
      if (!isIndexDoubleScan) {
        pushDownLimit(dagRequestBuilder, executorBuilder);
      }
    }

    return dagRequestBuilder;
  }

  private void pushDownLimit(
      DAGRequest.Builder dagRequestBuilder, Executor.Builder executorBuilder) {
    Limit.Builder limitBuilder = Limit.newBuilder();
    limitBuilder.setLimit(getLimit());
    executorBuilder.setTp(ExecType.TypeLimit);
    dagRequestBuilder.addExecutors(executorBuilder.setLimit(limitBuilder));
    executorBuilder.clear();
    addPushDownLimits();
  }

  private void pushDownOrderBy(
      DAGRequest.Builder dagRequestBuilder,
      Executor.Builder executorBuilder,
      Map<String, Integer> colOffsetInFieldMap) {
    TopN.Builder topNBuilder = TopN.newBuilder();
    getOrderByItems()
        .forEach(
            tiByItem ->
                topNBuilder.addOrderBy(
                    com.pingcap.tidb.tipb.ByItem.newBuilder()
                        .setExpr(ProtoConverter.toProto(tiByItem.getExpr(), colOffsetInFieldMap))
                        .setDesc(tiByItem.isDesc())));
    executorBuilder.setTp(ExecType.TypeTopN);
    topNBuilder.setLimit(getLimit());
    dagRequestBuilder.addExecutors(executorBuilder.setTopN(topNBuilder));
    executorBuilder.clear();
    addPushDownOrderBys();
  }

  private void pushDownAggAndGroupBy(
      DAGRequest.Builder dagRequestBuilder,
      Executor.Builder executorBuilder,
      List<Integer> outputOffsets,
      Map<String, Integer> colOffsetInFieldMap) {
    Aggregation.Builder aggregationBuilder = Aggregation.newBuilder();
    getAggregates()
        .forEach(
            tiExpr ->
                aggregationBuilder.addAggFunc(ProtoConverter.toProto(tiExpr, colOffsetInFieldMap)));
    getGroupByItems()
        .forEach(
            tiByItem ->
                aggregationBuilder.addGroupBy(
                    ProtoConverter.toProto(tiByItem.getExpr(), colOffsetInFieldMap)));
    executorBuilder.setTp(ExecType.TypeAggregation);
    dagRequestBuilder.addExecutors(executorBuilder.setAggregation(aggregationBuilder));
    executorBuilder.clear();
    addPushDownGroupBys();
    addPushDownAggregates();

    // adding output offsets for aggs
    outputOffsets.clear();
    for (int i = 0; i < getAggregates().size(); i++) {
      outputOffsets.add(i);
    }

    // adding output offsets for group by
    int currentMaxOutputOffset = outputOffsets.get(outputOffsets.size() - 1) + 1;
    for (int i = 0; i < getGroupByItems().size(); i++) {
      outputOffsets.add(currentMaxOutputOffset + i);
    }
  }

  private boolean isExpressionCoveredByIndex(Expression expr) {
    Set<String> indexColumnRefSet =
        indexInfo
            .getIndexColumns()
            .stream()
            .filter(x -> !x.isPrefixIndex())
            .map(TiIndexColumn::getName)
            .collect(Collectors.toSet());
    return !isDoubleRead()
        && PredicateUtils.extractColumnRefFromExpression(expr)
            .stream()
            .map(ColumnRef::getName)
            .allMatch(indexColumnRefSet::contains);
  }

  private boolean isGroupByCoveredByIndex() {
    return isByItemCoveredByIndex(getGroupByItems());
  }

  private boolean isOrderByCoveredByIndex() {
    return isByItemCoveredByIndex(getOrderByItems());
  }

  private boolean isByItemCoveredByIndex(List<ByItem> byItems) {
    if (byItems.isEmpty()) {
      return false;
    }
    return byItems.stream().allMatch(x -> isExpressionCoveredByIndex(x.getExpr()));
  }

  private boolean isAggregateCoveredByIndex() {
    if (aggregates.isEmpty()) {
      return false;
    }
    return aggregates.stream().allMatch(this::isExpressionCoveredByIndex);
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
    // check encode type
    requireNonNull(dagRequest.getEncodeType());

    // A DAG request must has at least one executor.
    if (dagRequest.getExecutorsCount() < 1) {
      throw new DAGRequestException("Invalid executors count:" + dagRequest.getExecutorsCount());
    }
    // A DAG request must start with TableScan or IndexScan Executor
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

  public TiTableInfo getTableInfo() {
    return this.tableInfo;
  }

  public TiDAGRequest setTableInfo(TiTableInfo tableInfo) {
    this.tableInfo = requireNonNull(tableInfo, "tableInfo is null");
    setPhysicalId(tableInfo.getId());
    return this;
  }

  public long getPhysicalId() {
    return this.physicalId;
  }

  public TiDAGRequest setPhysicalId(long id) {
    this.physicalId = id;
    return this;
  }

  public TiIndexInfo getIndexInfo() {
    return indexInfo;
  }

  public TiDAGRequest setIndexInfo(TiIndexInfo indexInfo) {
    this.indexInfo = requireNonNull(indexInfo, "indexInfo is null");
    return this;
  }

  public void clearIndexInfo() {
    indexInfo = null;
    clearPushDownInfo();
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

  int getTimeZoneOffset() {
    return timeZoneOffset;
  }

  /**
   * set truncate mode
   *
   * @param mode truncate mode
   * @return a TiDAGRequest
   */
  TiDAGRequest setTruncateMode(TiDAGRequest.TruncateMode mode) {
    flags = requireNonNull(mode, "mode is null").mask(flags);
    return this;
  }

  @VisibleForTesting
  long getFlags() {
    return flags;
  }

  @VisibleForTesting
  public TiTimestamp getStartTs() {
    return startTs;
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

  public boolean isDistinct() {
    return distinct;
  }

  public TiDAGRequest setDistinct(boolean distinct) {
    this.distinct = distinct;
    return this;
  }

  public TiDAGRequest addAggregate(AggregateFunction expr) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(expr);
    return this;
  }

  List<AggregateFunction> getAggregates() {
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
    if (!column.isResolved()) {
      throw new UnsupportedOperationException(
          String.format("cannot add unresolved column %s to dag request", column.getName()));
    }
    fields.add(requireNonNull(column, "columnRef is null"));
    return this;
  }

  public List<ColumnRef> getFields() {
    return fields;
  }

  /** Required index columns for double read */
  private void addRequiredIndexDataType() {
    if (!tableInfo.isCommonHandle()) {
      indexDataTypes.add(requireNonNull(IntegerType.BIGINT, "dataType is null"));
    } else {
      for (TiIndexColumn col : tableInfo.getPrimaryKey().getIndexColumns()) {
        String c = col.getName();
        ColumnRef cr = ColumnRef.create(c, tableInfo.getColumn(c));
        indexDataTypes.add(cr.getDataType());
      }
    }
  }

  public List<DataType> getIndexDataTypes() {
    return indexDataTypes;
  }

  /**
   * set key range of scan
   *
   * @param ranges key range of scan
   */
  public TiDAGRequest addRanges(Map<Long, List<Coprocessor.KeyRange>> ranges) {
    idToRanges.putAll(requireNonNull(ranges, "KeyRange is null"));
    return this;
  }

  private void resetRanges() {
    idToRanges.clear();
  }

  public void resetFilters(List<Expression> filters) {
    this.filters.clear();
    this.filters.addAll(filters);
  }

  public List<Coprocessor.KeyRange> getRangesByPhysicalId(long physicalId) {
    return idToRanges.get(physicalId);
  }

  public Map<Long, List<Coprocessor.KeyRange>> getRangesMaps() {
    return idToRanges;
  }

  public TiDAGRequest addFilters(List<Expression> filters) {
    this.filters.addAll(requireNonNull(filters, "filters expr is null"));
    return this;
  }

  public List<Expression> getFilters() {
    return filters;
  }

  public void addDowngradeFilter(Expression filter) {
    this.downgradeFilters.add(requireNonNull(filter, "downgrade filter is null"));
  }

  public List<Expression> getDowngradeFilters() {
    return downgradeFilters;
  }

  private void addPushDownFilters() {
    // all filters will be pushed down
    // TODO: choose some filters to push down
    this.pushDownFilters.addAll(filters);
  }

  private List<Expression> getPushDownFilters() {
    return pushDownFilters;
  }

  private void addPushDownAggregates() {
    this.pushDownAggregates.addAll(aggregates);
  }

  public List<AggregateFunction> getPushDownAggregates() {
    return pushDownAggregates;
  }

  private void addPushDownGroupBys() {
    this.pushDownGroupBys.addAll(getGroupByItems());
  }

  public List<ByItem> getPushDownGroupBys() {
    return pushDownGroupBys;
  }

  private void addPushDownOrderBys() {
    this.pushDownOrderBys.addAll(getOrderByItems());
  }

  public List<ByItem> getPushDownOrderBys() {
    return pushDownOrderBys;
  }

  private void addPushDownLimits() {
    this.pushDownLimits = limit;
  }

  private int getPushDownLimits() {
    return pushDownLimits;
  }

  private void clearPushDownInfo() {
    indexDataTypes.clear();
    pushDownFilters.clear();
    pushDownAggregates.clear();
    pushDownGroupBys.clear();
    pushDownOrderBys.clear();
    pushDownLimits = 0;
  }

  /**
   * Check whether the DAG request has any aggregate expression.
   *
   * @return the boolean
   */
  public boolean hasPushDownAggregate() {
    return !getPushDownAggregates().isEmpty();
  }

  /**
   * Check whether the DAG request has any group by expression.
   *
   * @return the boolean
   */
  public boolean hasPushDownGroupBy() {
    return !getPushDownGroupBys().isEmpty();
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
  private boolean isCoveringIndexScan() {
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

  /** Get the estimated row count will be fetched from this request. */
  public double getEstimatedCount() {
    return estimatedCount;
  }

  /** Set the estimated row count will be fetched from this request. */
  public void setEstimatedCount(double estimatedCount) {
    this.estimatedCount = estimatedCount;
  }

  public void init(boolean readHandle) {
    if (readHandle) {
      buildIndexScan();
    } else {
      buildTableScan();
    }
  }

  private void init() {
    init(hasIndex());
  }

  public IndexScanType getIndexScanType() {
    if (hasIndex()) {
      if (isDoubleRead) {
        return IndexScanType.INDEX_SCAN;
      } else {
        return IndexScanType.COVERING_INDEX_SCAN;
      }
    } else {
      return IndexScanType.TABLE_SCAN;
    }
  }

  @Override
  public String toString() {
    return this.copy().toStringInternal();
  }

  private String toStringInternal() {
    init();
    StringBuilder sb = new StringBuilder();
    if (tableInfo != null) {
      sb.append(String.format("[table: %s] ", tableInfo.getName()));
    }

    boolean isIndexScan = false;
    switch (getIndexScanType()) {
      case INDEX_SCAN:
        sb.append("IndexScan");
        sb.append(String.format("[Index: %s] ", indexInfo.getName()));
        isIndexScan = true;
        break;
      case COVERING_INDEX_SCAN:
        sb.append("CoveringIndexScan");
        sb.append(String.format("[Index: %s] ", indexInfo.getName()));
        break;
      case TABLE_SCAN:
        sb.append("TableScan");
    }

    if (!getFields().isEmpty()) {
      sb.append(", Columns: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getFields());
    }

    if (isIndexScan && !getDowngradeFilters().isEmpty()) {
      sb.append(", Downgrade Filter: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getDowngradeFilters());
    }

    if (!isIndexScan && !getFilters().isEmpty()) {
      sb.append(", Residual Filter: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getFilters());
    }

    if (!getPushDownFilters().isEmpty()) {
      sb.append(", PushDown Filter: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getPushDownFilters());
    }

    // Key ranges might be also useful
    if (!getRangesMaps().isEmpty()) {
      sb.append(", KeyRange: [");
      if (tableInfo.isPartitionEnabled()) {
        getRangesMaps()
            .forEach(
                (key, value) -> {
                  for (Coprocessor.KeyRange v : value) {
                    sb.append(" partition: ").append(getPrunedPartName(key));
                    // LogDesensitization: show key range in coprocessor request in log
                    sb.append(KeyUtils.formatBytesUTF8(v));
                  }
                });
      } else {
        getRangesMaps()
            .values()
            .forEach(
                vList -> {
                  for (Coprocessor.KeyRange range : vList) {
                    // LogDesensitization: show key range in coprocessor request in log
                    sb.append(KeyUtils.formatBytesUTF8(range));
                  }
                });
      }
      sb.append("]");
    }

    if (!getPushDownFilters().isEmpty()) {
      sb.append(", Aggregates: ");
      Joiner.on(", ").skipNulls().appendTo(sb, getPushDownAggregates());
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
    sb.append(", startTs: ").append(startTs.getVersion());
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

  public TiDAGRequest copyReqWithPhysicalId(long id) {
    TiDAGRequest req = this.copy();
    req.setPhysicalId(id);
    List<Coprocessor.KeyRange> currentIdRange = req.getRangesByPhysicalId(id);
    req.resetRanges();
    Map<Long, List<Coprocessor.KeyRange>> rangeMap = new HashMap<>();
    rangeMap.put(id, currentIdRange);
    req.addRanges(rangeMap);
    return req;
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

  public enum IndexScanType {
    INDEX_SCAN,
    COVERING_INDEX_SCAN,
    TABLE_SCAN
  }

  public static class Builder {
    private final List<String> requiredCols = new ArrayList<>();
    private final List<Expression> filters = new ArrayList<>();
    private final List<ByItem> orderBys = new ArrayList<>();
    private final Map<Long, List<Coprocessor.KeyRange>> ranges = new HashMap<>();
    private TiTableInfo tableInfo;
    private long physicalId;
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
        ranges.put(
            tableInfo.getId(),
            ImmutableList.of(
                KeyRangeUtils.makeCoprocRange(start.toByteString(), end.toByteString())));
      } else {
        for (TiPartitionDef pDef : tableInfo.getPartitionInfo().getDefs()) {
          RowKey start = RowKey.createMin(pDef.getId());
          RowKey end = RowKey.createBeyondMax(pDef.getId());
          ranges.put(
              pDef.getId(),
              ImmutableList.of(
                  KeyRangeUtils.makeCoprocRange(start.toByteString(), end.toByteString())));
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
      setPhysicalId(tableInfo.getId());
      return this;
    }

    public Builder setPhysicalId(long id) {
      this.physicalId = id;
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
      req.setPhysicalId(physicalId);
      req.addRanges(ranges);
      req.addFilters(filters);
      // this request will push down all filters
      req.addPushDownFilters();
      if (!orderBys.isEmpty()) {
        orderBys.forEach(req::addOrderByItem);
      }
      if (limit != 0) {
        req.setLimit(limit);
      }
      requiredCols.forEach(c -> req.addRequiredColumn(ColumnRef.create(c, tableInfo.getColumn(c))));
      req.setStartTs(startTs);

      return req;
    }
  }
}
