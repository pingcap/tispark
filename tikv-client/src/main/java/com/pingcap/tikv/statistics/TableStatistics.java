package com.pingcap.tikv.statistics;

import java.util.HashMap;
import java.util.Map;

public class TableStatistics {
  private final long tableId;
  private final Map<Long, ColumnStatistics> columnsHistMap = new HashMap<>();
  private final Map<Long, IndexStatistics> indexHistMap = new HashMap<>();
  private long count; // Total row count in a table.
  private long modifyCount;// Total modify count in a table.
  private long version;
  private boolean pseudo;

  public TableStatistics(long tableId) {
    this.tableId = tableId;
  }

  public long getTableId() {
    return tableId;
  }

  public Map<Long, ColumnStatistics> getColumnsHistMap() {
    return columnsHistMap;
  }

  public Map<Long, IndexStatistics> getIndexHistMap() {
    return indexHistMap;
  }

  public long getCount() {
    return count;
  }

  public void setCount(long count) {
    this.count = count;
  }

  public long getModifyCount() {
    return modifyCount;
  }

  public void setModifyCount(long modifyCount) {
    this.modifyCount = modifyCount;
  }

  public long getVersion() {
    return version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public boolean isPseudo() {
    return pseudo;
  }

  public void setPseudo(boolean pseudo) {
    this.pseudo = pseudo;
  }
}
