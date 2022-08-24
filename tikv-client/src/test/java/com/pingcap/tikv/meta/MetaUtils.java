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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.meta;

import com.pingcap.tikv.meta.TiPartitionInfo.PartitionType;
import com.pingcap.tikv.types.DataType;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.tikv.common.exception.TiClientInternalException;
import org.tikv.shade.com.google.common.collect.ImmutableList;

public class MetaUtils {
  public static class TableBuilder {
    long autoId = 1;
    private boolean pkHandle;
    private boolean isCommonHandle;
    private String name;
    private final List<TiColumnInfo> columns = new ArrayList<>();
    private final List<TiIndexInfo> indices = new ArrayList<>();
    private TiPartitionInfo partInfo;
    private Long tid = null;
    private long version = 0L;
    private final long updateTimestamp = 0L;
    private final long maxShardRowIDBits = 0L;
    private final long autoRandomBits = 0L;

    public TableBuilder() {}

    public static TableBuilder newBuilder() {
      return new TableBuilder();
    }

    private long newId() {
      return autoId++;
    }

    public TableBuilder name(String name) {
      this.name = name;
      return this;
    }

    public TableBuilder tableId(long id) {
      this.tid = id;
      return this;
    }

    public TableBuilder addColumn(String name, DataType type) {
      return addColumn(name, type, false);
    }

    public TableBuilder addColumn(String name, DataType type, long colId) {
      return addColumn(name, type, false, colId);
    }

    public TableBuilder addColumn(String name, DataType type, boolean pk, long colId) {
      for (TiColumnInfo c : columns) {
        if (c.matchName(name)) {
          throw new TiClientInternalException("duplicated name: " + name);
        }
      }

      TiColumnInfo col = new TiColumnInfo(colId, name, columns.size(), type, pk);
      columns.add(col);
      return this;
    }

    public TableBuilder addColumn(String name, DataType type, boolean pk) {
      return addColumn(name, type, pk, newId());
    }

    public TableBuilder addPartition(
        String expr, PartitionType type, List<TiPartitionDef> partitionDefs, List<CIStr> columns) {
      this.partInfo =
          new TiPartitionInfo(
              TiPartitionInfo.partTypeToLong(type), expr, columns, true, partitionDefs);
      return this;
    }

    public TableBuilder appendIndex(
        long iid, String indexName, List<String> colNames, boolean isPk) {
      List<TiIndexColumn> indexCols =
          colNames
              .stream()
              .map(name -> columns.stream().filter(c -> c.matchName(name)).findFirst())
              .flatMap(col -> col.isPresent() ? Stream.of(col.get()) : Stream.empty())
              .map(TiColumnInfo::toIndexColumn)
              .collect(Collectors.toList());

      TiIndexInfo index =
          new TiIndexInfo(
              iid,
              CIStr.newCIStr(indexName),
              CIStr.newCIStr(name),
              ImmutableList.copyOf(indexCols),
              false,
              isPk,
              SchemaState.StatePublic.getStateCode(),
              "",
              IndexType.IndexTypeBtree.getTypeCode(),
              false,
              false);
      indices.add(index);
      return this;
    }

    public TableBuilder appendIndex(String indexName, List<String> colNames, boolean isPk) {
      return appendIndex(newId(), indexName, colNames, isPk);
    }

    public TableBuilder setPkHandle(boolean pkHandle) {
      this.pkHandle = pkHandle;
      return this;
    }

    public TableBuilder setIsCommonHandle(boolean isCommonHandle) {
      this.isCommonHandle = isCommonHandle;
      return this;
    }

    public TableBuilder version(long version) {
      this.version = version;
      return this;
    }

    public TiTableInfo build() {
      if (tid == null) {
        tid = newId();
      }
      if (name == null) {
        name = "Table" + tid;
      }
      return new TiTableInfo(
          tid,
          CIStr.newCIStr(name),
          "",
          "",
          pkHandle,
          isCommonHandle,
          1,
          columns,
          indices,
          "",
          0,
          0,
          0,
          0,
          partInfo,
          null,
          null,
          version,
          updateTimestamp,
          maxShardRowIDBits,
          null,
          autoRandomBits);
    }
  }
}
