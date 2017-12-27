package com.pingcap.tikv.meta;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.pingcap.tidb.tipb.*;
import com.pingcap.tikv.exception.DAGRequestException;
import com.pingcap.tikv.exception.TiClientInternalException;
import com.pingcap.tikv.expression.TiByItem;
import com.pingcap.tikv.expression.TiColumnRef;
import com.pingcap.tikv.expression.TiExpr;
import com.pingcap.tikv.expression.TiFunctionExpression;
import com.pingcap.tikv.kvproto.Coprocessor;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.KeyRangeUtils;
import com.pingcap.tikv.util.Pair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.pingcap.tikv.predicates.PredicateUtils.mergeCNFExpressions;
import static java.util.Objects.requireNonNull;

/**
 * Type TiDAGRequest.
 * <p>
 * Used for constructing a new DAG request to TiKV
 */
public class TiDAGRequest implements Serializable {
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

  /**
   * Whether we use streaming to push down the request
   */
  public enum PushDownType {
    STREAMING,
    NORMAL
  }

  /**
   * Predefined executor priority map.
   */
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
  private TiIndexInfo indexInfo;
  private final List<TiColumnRef> fields = new ArrayList<>();
  private final List<TiExpr> filter = new ArrayList<>();
  private final List<TiByItem> groupByItems = new ArrayList<>();
  private final List<TiByItem> orderByItems = new ArrayList<>();
  // System like Spark has different type promotion rules
  // we need a cast to target when given
  private final List<Pair<TiExpr, DataType>> aggregates = new ArrayList<>();
  private final List<Coprocessor.KeyRange> keyRanges = new ArrayList<>();
  // If index scanning of this request is not possible in some scenario, we downgrade it to a table scan and use
  // downGradeRanges instead of index scan ranges stored in keyRanges along with downgradeFilters to perform a
  // table scan.
  private List<TiExpr> downgradeFilters = new ArrayList<>();

  private int limit;
  private int timeZoneOffset;
  private long flags;
  private long startTs;
  private TiExpr having;
  private boolean distinct;
  private boolean handleNeeded;
  private final PushDownType pushDownType;

  public void resolve() {
    getFields().forEach(expr -> expr.resolve(tableInfo));
    getFilter().forEach(expr -> expr.resolve(tableInfo));
    getGroupByItems().forEach(item -> item.getExpr().resolve(tableInfo));
    getOrderByItems().forEach(item -> item.getExpr().resolve(tableInfo));
    getAggregates().forEach(expr -> expr.resolve(tableInfo));
    if (having != null) {
      having.resolve(tableInfo);
    }
  }

  public DAGRequest buildScan(boolean idxScan) {
    if (idxScan) {
      return buildIndexScan();
    } else {
      return buildTableScan();
    }
  }

  private DAGRequest buildIndexScan() {
    checkArgument(startTs != 0, "timestamp is 0");
    if (indexInfo == null) {
      throw new TiClientInternalException("Index is empty for index scan");
    }
    DAGRequest.Builder dagRequestBuilder = DAGRequest.newBuilder();
    Executor.Builder executorBuilder = Executor.newBuilder();
    IndexScan.Builder indexScanBuilder = IndexScan.newBuilder();

    List<TiColumnInfo> columnInfoList = tableInfo.getColumns();
    boolean hasPk = false;
    // We extract index column info
    List<Integer> indexColIds = indexInfo
        .getIndexColumns()
        .stream()
        .map(TiIndexColumn::getOffset)
        .collect(Collectors.toList());

    for (Integer idx : indexColIds) {
      ColumnInfo columnInfo = columnInfoList
          .get(idx)
          .toProto(tableInfo);

      ColumnInfo.Builder colBuilder = ColumnInfo.newBuilder();
      colBuilder.setTp(columnInfo.getTp());
      colBuilder.setColumnId(columnInfo.getColumnId());
      colBuilder.setCollation(columnInfo.getCollation());
      colBuilder.setColumnLen(columnInfo.getColumnLen());
      colBuilder.setFlag(columnInfo.getFlag());
      if (columnInfo.getColumnId() == -1) {
        hasPk = true;
        colBuilder.setPkHandle(true);
      }
      indexScanBuilder.addColumns(colBuilder);
    }

    if (!hasPk) {
      ColumnInfo handleColumn = ColumnInfo.newBuilder()
          .setColumnId(-1)
          .setPkHandle(true)
          // We haven't changed the field name in protobuf file, but
          // we need to set this to true in order to retrieve the handle,
          // so the name 'setPkHandle' may sounds strange.
          .build();
      indexScanBuilder.addColumns(handleColumn);
    }
    executorBuilder.setTp(ExecType.TypeIndexScan);

    indexScanBuilder
        .setTableId(tableInfo.getId())
        .setIndexId(indexInfo.getId());
    dagRequestBuilder.addExecutors(executorBuilder.setIdxScan(indexScanBuilder).build());
    int colCount = indexScanBuilder.getColumnsCount();
    dagRequestBuilder.addOutputOffsets(
        colCount != 0 ? colCount - 1 : 0
    );
    return dagRequestBuilder
        .setFlags(flags)
        .setTimeZoneOffset(timeZoneOffset)
        .setStartTs(startTs)
        .build();
  }

  /**
   * @return DAGRequest built
   */
  private DAGRequest buildTableScan() {
    checkArgument(startTs != 0, "timestamp is 0");
    DAGRequest.Builder dagRequestBuilder = DAGRequest.newBuilder();
    Executor.Builder executorBuilder = Executor.newBuilder();
    TableScan.Builder tblScanBuilder = TableScan.newBuilder();
    // Step1. Add columns to first executor
    getFields().forEach(tiColumnInfo ->
        tblScanBuilder.addColumns(
            tiColumnInfo.getColumnInfo().toProto(tableInfo)
        )
    );
    executorBuilder.setTp(ExecType.TypeTableScan);
    tblScanBuilder.setTableId(tableInfo.getId());
    // Currently, according to TiKV's implementation, if handle
    // is needed, we should add an extra column with an ID of -1
    // to the TableScan executor
    if (isHandleNeeded()) {
      ColumnInfo handleColumn = ColumnInfo.newBuilder()
          .setColumnId(-1)
          .setPkHandle(true)
          // We haven't changed the field name in protobuf file, but
          // we need to set this to true in order to retrieve the handle,
          // so the name 'setPkHandle' may sounds strange.
          .build();
      tblScanBuilder.addColumns(handleColumn);
    }
    dagRequestBuilder.addExecutors(executorBuilder.setTblScan(tblScanBuilder));
    executorBuilder.clear();

    // Step2. Add others
    // DO NOT EDIT EXPRESSION CONSTRUCTION ORDER
    // Or make sure the construction order is below:
    // TableScan/IndexScan > Selection > Aggregation > TopN/Limit
    TiExpr filterExpr = mergeCNFExpressions(
        getFilter().stream().peek(this::setColumnOffsets).collect(Collectors.toList())
    );

    if (filterExpr != null) {
      executorBuilder.setTp(ExecType.TypeSelection);
      dagRequestBuilder.addExecutors(
          executorBuilder.setSelection(
              Selection.newBuilder().addConditions(filterExpr.toProto())
          )
      );
      executorBuilder.clear();
    }

    if (!getGroupByItems().isEmpty() || !getAggregates().isEmpty()) {
      Aggregation.Builder aggregationBuilder = Aggregation.newBuilder();
      getGroupByItems().stream().map(TiByItem::getExpr).forEach(this::setColumnOffsets);
      getGroupByItems().forEach(tiByItem -> aggregationBuilder.addGroupBy(tiByItem.getExpr().toProto()));
      getAggregates().forEach(this::setColumnOffsets);
      getAggregates().forEach(tiExpr -> aggregationBuilder.addAggFunc(tiExpr.toProto()));
      executorBuilder.setTp(ExecType.TypeAggregation);
      dagRequestBuilder.addExecutors(
          executorBuilder.setAggregation(aggregationBuilder)
      );
      executorBuilder.clear();
    }

    if (!getOrderByItems().isEmpty()) {
      TopN.Builder topNBuilder = TopN.newBuilder();
      getOrderByItems().stream().map(TiByItem::getExpr).forEach(this::setColumnOffsets);
      getOrderByItems().forEach(tiByItem -> topNBuilder.addOrderBy(tiByItem.toProto()));
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

    // column offset should be in accordance with the
    for (int i = 0; i < getFields().size(); i++) {
      dagRequestBuilder.addOutputOffsets(i);
    }
    // if handle is needed, we should append one output offset
    if (isHandleNeeded()) {
      dagRequestBuilder.addOutputOffsets(tableInfo.getColumns().size());
    }

    DAGRequest request = dagRequestBuilder
        .setTimeZoneOffset(timeZoneOffset)
        .setFlags(flags)
        .setStartTs(startTs)
        .build();

    validateRequest(request);

    return request;
  }

  private void setColumnOffsets(TiExpr expr) {
    if (expr instanceof TiFunctionExpression) {
      ((TiFunctionExpression) expr).getArgs().forEach(
          this::setColumnOffsets
      );
    } else if (expr instanceof TiColumnRef) {
      TiColumnRef columnRef = (TiColumnRef) expr;
      long targetId = columnRef.getColumnInfo().getId();
      int pos = 0;
      // Set offset of each Column according to the ordering
      // of fields.
      for (TiColumnRef col : getFields()) {
        if (col.getColumnInfo().getId() == targetId) {
          break;
        }
        pos++;
      }

      if (getFields().size() == pos) {
        throw new DAGRequestException("No column match id:" + targetId);
      }
      columnRef.setOffset(pos);
    }
  }

  public boolean isIndexScan() {
    return indexInfo != null;
  }

  /**
   * Check if a DAG request is valid.
   *
   * Note:
   * When constructing a DAG request, a executor with an ExecType of higher priority
   * should always be placed before those lower ones.
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
    if (formerType != ExecType.TypeTableScan &&
        formerType != ExecType.TypeIndexScan) {
      throw new DAGRequestException("Invalid first executor type:" + formerType +
          ", must one of TypeTableScan or TypeIndexScan");
    }

    for (int i = 1; i < dagRequest.getExecutorsCount(); i++) {
      ExecType currentType = dagRequest.getExecutors(i).getTp();
      if (EXEC_TYPE_PRIORITY_MAP.get(currentType) <
          EXEC_TYPE_PRIORITY_MAP.get(formerType)) {
        throw new DAGRequestException("Invalid executor priority.");
      }
      if (currentType.equals(ExecType.TypeTopN)) {
        long limit = dagRequest.getExecutors(i).getTopN().getLimit();
        if (limit == 0) {
          throw new DAGRequestException("TopN executor should contain non-zero limit number but received:" + limit);
        }
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

  public TiDAGRequest setIndexInfo(TiIndexInfo indexInfo) {
    this.indexInfo = requireNonNull(indexInfo, "indexInfo is null");
    return this;
  }

  TiIndexInfo getIndexInfo() {
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
  public long getFlags() {
    return flags;
  }

  /**
   * set start timestamp for the transaction
   *
   * @param startTs timestamp
   * @return a TiDAGRequest
   */
  public TiDAGRequest setStartTs(long startTs) {
    this.startTs = startTs;
    return this;
  }

  long getStartTs() {
    return startTs;
  }

  /**
   * set having clause to select query
   *
   * @param having is a expression represents Having
   * @return a TiDAGRequest
   */
  public TiDAGRequest setHaving(TiExpr having) {
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

  /**
   * add aggregate function to select query
   *
   * @param expr is a TiUnaryFunction expression.
   * @return a SelectBuilder
   */
  public TiDAGRequest addAggregate(TiExpr expr) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(Pair.create(expr, expr.getType()));
    return this;
  }

  public TiDAGRequest addAggregate(TiExpr expr, DataType targetType) {
    requireNonNull(expr, "aggregation expr is null");
    aggregates.add(Pair.create(expr, targetType));
    return this;
  }

  public List<TiExpr> getAggregates() {
    return aggregates.stream().map(p -> p.first).collect(Collectors.toList());
  }

  public List<Pair<TiExpr, DataType>> getAggregatePairs() {
    return aggregates;
  }

  /**
   * add a order by clause to select query.
   *
   * @param byItem is a TiByItem.
   * @return a SelectBuilder
   */
  public TiDAGRequest addOrderByItem(TiByItem byItem) {
    orderByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  List<TiByItem> getOrderByItems() {
    return orderByItems;
  }

  /**
   * add a group by clause to select query
   *
   * @param byItem is a TiByItem
   * @return a SelectBuilder
   */
  public TiDAGRequest addGroupByItem(TiByItem byItem) {
    groupByItems.add(requireNonNull(byItem, "byItem is null"));
    return this;
  }

  public List<TiByItem> getGroupByItems() {
    return groupByItems;
  }

  /**
   * Field is not support in TiDB yet, for here we simply allow TiColumnRef instead of TiExpr like
   * in SelectRequest proto
   * <p>
   * <p>This interface allows duplicate columns and it's user's responsibility to do dedup since we
   * need to ensure exact order and items preserved during decoding
   *
   * @param column is column referred during selectReq
   */
  public TiDAGRequest addRequiredColumn(TiColumnRef column) {
    fields.add(requireNonNull(column, "columnRef is null"));
    return this;
  }

  public List<TiColumnRef> getFields() {
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

  public void resetFilters(List<TiExpr> filters) {
    filter.clear();
    filter.addAll(filters);
  }

  public List<Coprocessor.KeyRange> getRanges() {
    return keyRanges;
  }

  public TiDAGRequest addFilter(TiExpr filter) {
    this.filter.add(requireNonNull(filter, "filter expr is null"));
    return this;
  }

  public TiDAGRequest addDowngradeFilter(TiExpr filter) {
    this.downgradeFilters.add(requireNonNull(filter, "downgrade filter is null"));
    return this;
  }

  /**
   * Check whether the DAG request has any
   * aggregate expression.
   *
   * @return the boolean
   */
  public boolean hasAggregate() {
    return !getAggregates().isEmpty();
  }

  /**
   * Check whether the DAG request has any
   * group by expression.
   *
   * @return the boolean
   */
  public boolean hasGroupBy() {
    return !getGroupByItems().isEmpty();
  }

  public List<TiExpr> getFilter() {
    return filter;
  }

  public List<TiExpr> getDowngradeFilters() {
    return downgradeFilters;
  }

  public List<TiColumnInfo> getColInfoList() {
    return getFields().stream().map(TiColumnRef::getColumnInfo).collect(Collectors.toList());
  }

  /**
   * Gets group by dt list.
   *
   * @return the group by dt list
   */
  public List<DataType> getGroupByDTList() {
    return getGroupByItems()
        .stream()
        .map(TiByItem::getExpr)
        .map(TiExpr::getType)
        .collect(Collectors.toList());
  }

  /**
   * Returns whether handle is needed.
   *
   * @return the boolean
   */
  public boolean isHandleNeeded() {
    return handleNeeded;
  }

  /**
   * Sets handle needed.
   *
   * @param handleNeeded the handle needed
   */
  public void setHandleNeeded(boolean handleNeeded) {
    this.handleNeeded = handleNeeded;
  }

  /**
   * Whether we use streaming processing to retrieve data
   *
   * @return push down type.
   */
  public PushDownType getPushDownType() {
    return pushDownType;
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

    if (getRanges().size() != 0) {
      sb.append(", Ranges: ");
      List<String> rangeStrings = getRanges()
          .stream()
          .map(KeyRangeUtils::toString)
          .collect(Collectors.toList());
      sb.append(Joiner.on(", ").skipNulls().join(rangeStrings));
    }

    if (getFields().size() != 0) {
      sb.append(", Columns: ");
      sb.append(Joiner.on(", ").skipNulls().join(getFields()));
    }

    if (getFilter().size() != 0) {
      sb.append(", Filter: ");
      sb.append(Joiner.on(", ").skipNulls().join(getFilter()));
    }

    if (getAggregates().size() != 0) {
      sb.append(", Aggregates: ");
      sb.append(Joiner.on(", ").skipNulls().join(getAggregates()));
    }

    if (getGroupByItems().size() != 0) {
      sb.append(", Group By: ");
      sb.append(Joiner.on(", ").skipNulls().join(getGroupByItems()));
    }

    if (getOrderByItems().size() != 0) {
      sb.append(", Order By: ");
      sb.append(Joiner.on(", ").skipNulls().join(getOrderByItems()));
    }

    if (getLimit() != 0) {
      sb.append(", Limit: ");
      sb.append("[").append(limit).append("]");
    }

    return sb.toString();
  }

}
