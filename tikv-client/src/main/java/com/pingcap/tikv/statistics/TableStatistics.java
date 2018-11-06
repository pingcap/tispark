/*
 *
 * Copyright 2018 PingCAP, Inc.
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
 *
 */

package com.pingcap.tikv.statistics;

import java.util.HashMap;
import java.util.Map;

/**
 * A TableStatistics Java plain object.
 *
 * <p>Usually each table will have two types of statistics information: 1. Meta info (tableId,
 * count, modifyCount, version) 2. Column/Index histogram info (columnsHistMap, indexHistMap)
 */
public class TableStatistics {
  private final long tableId; // Which table it belongs to
  private final Map<Long, ColumnStatistics> columnsHistMap =
      new HashMap<>(); // ColumnId -> ColumnStatistics map
  private final Map<Long, IndexStatistics> indexHistMap =
      new HashMap<>(); // IndexId -> IndexStatistics map
  private long count; // Total row count in a table.
  private long modifyCount; // Total modify count in a table.
  private long version; // Version of this statistics info

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
}
