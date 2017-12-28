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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tidb.tipb.IndexInfo;
import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

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

  @JsonCreator
  TiIndexInfo(
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
      @JsonProperty("___isFakePrimaryKey") boolean isFakePrimaryKey) {
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
  }

  public static TiIndexInfo generateFakePrimaryKeyIndex(TiTableInfo table) {
    TiColumnInfo pkColumn = table.getPrimaryKeyColumn();
    if (pkColumn != null) {
      return new TiIndexInfo(
          -1,
          CIStr.newCIStr("fake_pk_" + table.getId()),
          CIStr.newCIStr(table.getName()),
          ImmutableList.of(pkColumn.toIndexColumn()),
          true,
          true,
          SchemaState.StatePublic.getStateCode(),
          "Fake Column",
          IndexType.IndexTypeHash.getTypeCode(),
          true);
    }
    return null;
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
        ColumnInfo pbColumn = column.toProtoBuilder(tableInfo).setPkHandle(true).build();
        builder.addColumns(pbColumn);
      }
    }
    return builder.build();
  }

  public boolean isFakePrimaryKey() {
    return isFakePrimaryKey;
  }

  @Override
  public String toString() {
    return String.format("%s[%s]",
        name,
        Joiner.on(",")
            .skipNulls()
            .join(indexColumns
                .stream()
                .map(column -> column.getName())
                .collect(Collectors.toList())
            )
    );
  }
}
