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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.pingcap.tidb.tipb.ColumnInfo;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;
import com.pingcap.tikv.types.DataTypeFactory;
import com.pingcap.tikv.types.IntegerType;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiColumnInfo implements Serializable {
  private final long id;
  private final String name;
  private final int offset;
  private final DataType type;
  private final SchemaState schemaState;
  private final String comment;
  private final boolean isPrimaryKey;
  private final String defaultValue;
  private final String originDefaultValue;

  public static final TiColumnInfo ROW_ID =
      new TiColumnInfo(-1, "_tidb_rowid", 0, IntegerType.BIGINT, true);

  @VisibleForTesting
  private static final int PK_MASK = 0x2;

  @JsonCreator
  public TiColumnInfo(
      @JsonProperty("id") long id,
      @JsonProperty("name") CIStr name,
      @JsonProperty("offset") int offset,
      @JsonProperty("type") InternalTypeHolder type,
      @JsonProperty("state") int schemaState,
      @JsonProperty("origin_default") String originalDefaultValue,
      @JsonProperty("default") String defaultValue,
      @JsonProperty("comment") String comment) {
    this.id = id;
    this.name = requireNonNull(name, "column name is null").getL();
    this.offset = offset;
    this.type = DataTypeFactory.of(requireNonNull(type, "type is null"));
    this.schemaState = SchemaState.fromValue(schemaState);
    this.comment = comment;
    this.defaultValue = defaultValue;
    this.originDefaultValue = originalDefaultValue;
    // I don't think pk flag should be set on type
    // Refactor against original tidb code
    this.isPrimaryKey = (type.getFlag() & PK_MASK) > 0;
  }

  @VisibleForTesting
  public TiColumnInfo(long id, String name, int offset, DataType type, boolean isPrimaryKey) {
    this.id = id;
    this.name = requireNonNull(name, "column name is null").toLowerCase();
    this.offset = offset;
    this.type = requireNonNull(type, "data type is null");
    this.schemaState = SchemaState.StatePublic;
    this.comment = "";
    this.isPrimaryKey = isPrimaryKey;
    this.originDefaultValue = "1";
    this.defaultValue = "";
  }

  public long getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }

  public boolean matchName(String name) {
    return this.name.equalsIgnoreCase(name);
  }

  public int getOffset() {
    return this.offset;
  }

  public DataType getType() {
    return type;
  }

  public SchemaState getSchemaState() {
    return schemaState;
  }

  public String getComment() {
    return comment;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public ByteString getOriginDefaultValue() {
    CodecDataOutput cdo = new CodecDataOutput();
    type.encode(cdo, EncodeType.VALUE, type.getOriginDefaultValue(originDefaultValue));
    return cdo.toByteString();
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class InternalTypeHolder {
    private final int tp;
    private final int flag;
    private final long flen;
    private final int decimal;
    private final String charset;
    private final String collate;
    private final String defaultValue;
    private final String originDefaultValue;
    private final List<String> elems;

    public String getDefaultValue() {
      return defaultValue;
    }

    public String getOriginDefaultValue() {
      return originDefaultValue;
    }

    interface Builder<E extends DataType> {
      E build(InternalTypeHolder holder);
    }

    @JsonCreator
    public InternalTypeHolder(
        @JsonProperty("Tp") int tp,
        @JsonProperty("Flag") int flag,
        @JsonProperty("Flen") long flen,
        @JsonProperty("Decimal") int decimal,
        @JsonProperty("Charset") String charset,
        @JsonProperty("origin_default") String originalDefaultValue,
        @JsonProperty("default") String defaultValue,
        @JsonProperty("Collate") String collate,
        @JsonProperty("Elems") List<String> elems) {
      this.tp = tp;
      this.flag = flag;
      this.flen = flen;
      this.decimal = decimal;
      this.charset = charset;
      this.collate = collate;
      this.defaultValue = defaultValue;
      this.originDefaultValue = originalDefaultValue;
      this.elems = elems;
    }

    public InternalTypeHolder(ColumnInfo c) {
      this.tp = c.getTp();
      this.flag = c.getFlag();
      this.flen = c.getColumnLen();
      this.decimal = c.getDecimal();
      this.charset = "";
      this.collate = Collation.translate(c.getCollation());
      this.elems = c.getElemsList();
      this.defaultValue = c.getDefaultVal().toStringUtf8();
      // TODO: we may need write a functon about get origin default value according to the string.
      this.originDefaultValue = "";
    }

    public int getTp() {
      return tp;
    }

    public int getFlag() {
      return flag;
    }

    public long getFlen() {
      return flen;
    }

    public int getDecimal() {
      return decimal;
    }

    public String getCharset() {
      return charset;
    }

    public String getCollate() {
      return collate;
    }

    public List<String> getElems() {
      return elems;
    }
  }

  TiIndexColumn toFakeIndexColumn() {
    // we don't use original length of column since for a clustered index column
    // it always full index instead of prefix index
    return new TiIndexColumn(CIStr.newCIStr(getName()), getOffset(), DataType.UNSPECIFIED_LEN);
  }

  TiIndexColumn toIndexColumn() {
    return new TiIndexColumn(CIStr.newCIStr(getName()), getOffset(), getType().getLength());
  }

  public ColumnInfo toProto(TiTableInfo table) {
    return toProtoBuilder(table).build();
  }

  ColumnInfo.Builder toProtoBuilder(TiTableInfo table) {
    return ColumnInfo.newBuilder()
        .setColumnId(id)
        .setTp(type.getTypeCode())
        .setCollation(type.getCollationCode())
        .setColumnLen((int) type.getLength())
        .setDecimal(type.getDecimal())
        .setFlag(type.getFlag())
        .setDefaultVal(getOriginDefaultValue())
        .setPkHandle(table.isPkHandle() && isPrimaryKey())
        .addAllElems(type.getElems());
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof TiColumnInfo)) {
      return false;
    }

    TiColumnInfo col = (TiColumnInfo)other;
    return Objects.equals(id, col.id) &&
        Objects.equals(name, col.name) &&
        Objects.equals(type, col.type) &&
        Objects.equals(schemaState, col.schemaState) &&
        isPrimaryKey == col.isPrimaryKey &&
        Objects.equals(defaultValue, col.defaultValue) &&
        Objects.equals(originDefaultValue, col.originDefaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, name, type,
        schemaState, isPrimaryKey,
        defaultValue, originDefaultValue);
  }
}
