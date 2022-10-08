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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tidb.tipb.IndexInfo;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;
import org.tikv.common.exception.TiKVException;
import org.tikv.shade.com.google.common.annotations.VisibleForTesting;
import org.tikv.shade.com.google.common.base.Joiner;
import org.tikv.shade.com.google.common.collect.ImmutableList;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiIndexInfo implements Serializable {
  private final long id;
  private final String name;
  private final String tableName;
  private final List<TiIndexColumn> indexColumns;
  private final boolean isUnique;
  private final boolean isPrimary;
  private final SchemaState schemaState;
  private final String comment;
  private final IndexType indexType;
  private final boolean isFakePrimaryKey;
  private final boolean isInvisible;

  // default index column size (TypeFlag + Int64)
  private long indexColumnSize = 9;

  @JsonCreator
  @VisibleForTesting
  public TiIndexInfo(
      @JsonProperty("id") long id,
      @JsonProperty("idx_name") CIStr name,
      @JsonProperty("tbl_name") CIStr tableName,
      @JsonProperty("idx_cols") List<TiIndexColumn> indexColumns,
      @JsonProperty("is_unique") boolean isUnique,
      @JsonProperty("is_primary") boolean isPrimary,
      @JsonProperty("state") int schemaState,
      @JsonProperty("comment") String comment,
      @JsonProperty("index_type") int indexType,
      // This is a fake property and added JsonProperty only to
      // to bypass Jackson frameworks's check
      @JsonProperty("___isFakePrimaryKey") boolean isFakePrimaryKey,
      @JsonProperty("is_invisible") boolean isInvisible) {
    this.id = id;
    this.name = requireNonNull(name, "index name is null").getL();
    this.tableName = requireNonNull(tableName, "table name is null").getL();
    this.indexColumns = ImmutableList.copyOf(requireNonNull(indexColumns, "indexColumns is null"));
    this.isUnique = isUnique;
    this.isPrimary = isPrimary;
    this.schemaState = SchemaState.fromValue(schemaState);
    this.comment = comment;
    this.indexType = IndexType.fromValue(indexType);
    this.isFakePrimaryKey = isFakePrimaryKey;
    this.isInvisible = isInvisible;
  }

  // To reuse the logic of buildIndexScan, we wrap the primary key as an index here.
  public static TiIndexInfo genClusterIndex(TiTableInfo table) {
    if (table.isPkHandle() || table.isCommonHandle()) {
      ImmutableList<TiIndexColumn> columns;
      if (table.isPkHandle()) {
        TiColumnInfo pkColumn = table.getPKIsHandleColumn();
        // The integer handle has no prefix when store in to Tikv.
        columns = ImmutableList.of(pkColumn.toUnSpecifiedLenIndexColumn());
      } else {
        // make the len of cols which don't have prefix to UNSPECIFIED_LEN.
        columns = ImmutableList.copyOf(table.convertIndexColToPrefixCols(table.getPrimaryKey()));
      }
      return new TiIndexInfo(
          -1,
          CIStr.newCIStr("cluster_index_" + table.getId()),
          CIStr.newCIStr(table.getName()),
          columns,
          true,
          true,
          SchemaState.StatePublic.getStateCode(),
          "Cluster Index",
          IndexType.IndexTypeHash.getTypeCode(),
          true,
          false);
    }
    return null;
  }

  private long calculateIndexColumnSize(TiIndexColumn indexColumn, List<TiColumnInfo> columns) {
    for (TiColumnInfo column : columns) {
      if (column.getName().equalsIgnoreCase(indexColumn.getName())) {
        return column.getType().getSize();
      }
    }
    throw new TiKVException(
        String.format(
            "Index column [%s] not found in table [%s] columns [%s]",
            indexColumn.getName(), getTableName(), columns));
  }

  void calculateIndexSize(List<TiColumnInfo> columns) {
    long ret = 0;
    for (TiIndexColumn indexColumn : indexColumns) {
      if (indexColumn.isLengthUnspecified()) {
        ret += calculateIndexColumnSize(indexColumn, columns);
      } else {
        ret += indexColumn.getLength();
      }
    }
    indexColumnSize = ret;
  }

  public long getIndexColumnSize() {
    return this.indexColumnSize;
  }

  public long getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getTableName() {
    return tableName;
  }

  public List<TiIndexColumn> getIndexColumns() {
    return indexColumns;
  }

  public boolean isUnique() {
    return isUnique;
  }

  public boolean isPrimary() {
    return isPrimary;
  }

  public SchemaState getSchemaState() {
    return schemaState;
  }

  public String getComment() {
    return comment;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public IndexInfo toProto(TiTableInfo tableInfo) {
    IndexInfo.Builder builder =
        IndexInfo.newBuilder().setTableId(tableInfo.getId()).setIndexId(id).setUnique(isUnique);

    List<TiColumnInfo> columns = tableInfo.getColumns();

    for (TiIndexColumn indexColumn : getIndexColumns()) {
      int offset = indexColumn.getOffset();
      TiColumnInfo column = columns.get(offset);
      builder.addColumns(column.toProto(tableInfo));
    }

    if (tableInfo.isPkHandle()) {
      for (TiColumnInfo column : columns) {
        if (!column.isPrimaryKey()) {
          continue;
        }
        ColumnInfo pbColumn = column.toProto(tableInfo);
        builder.addColumns(pbColumn);
      }
    }
    return builder.build();
  }

  public boolean isFakePrimaryKey() {
    return isFakePrimaryKey;
  }

  public boolean isInvisible() {
    return isInvisible;
  }

  @Override
  public String toString() {
    return String.format(
        "%s[%s]",
        name,
        Joiner.on(",")
            .skipNulls()
            .join(indexColumns.stream().map(TiIndexColumn::getName).collect(Collectors.toList())));
  }
}
