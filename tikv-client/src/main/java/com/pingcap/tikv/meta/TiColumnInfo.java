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
import com.pingcap.tikv.types.MySQLType;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TiColumnInfo implements Serializable {
  @VisibleForTesting private static final int PK_MASK = 0x2;
  private final long id;
  private final String name;
  private final int offset;
  private final DataType type;
  private final SchemaState schemaState;
  private final String comment;
  private final boolean isPrimaryKey;
  private final String defaultValue;
  private final String originDefaultValue;
  private final String defaultValueBit;
  // this version is from ColumnInfo which is used to address compatible issue.
  // If version is 0 then timestamp's default value will be read and decoded as local timezone.
  // if version is 1 then timestamp's default value will be read and decoded as utc.
  private final long version;
  private final String generatedExprString;
  private final boolean hidden;

  @JsonCreator
  public TiColumnInfo(
      @JsonProperty("id") long id,
      @JsonProperty("name") CIStr name,
      @JsonProperty("offset") int offset,
      @JsonProperty("type") InternalTypeHolder type,
      @JsonProperty("state") int schemaState,
      @JsonProperty("origin_default") String originalDefaultValue,
      @JsonProperty("default") String defaultValue,
      @JsonProperty("default_bit") String defaultValueBit,
      @JsonProperty("comment") String comment,
      @JsonProperty("version") long version,
      @JsonProperty("generated_expr_string") String generatedExprString,
      @JsonProperty("hidden") boolean hidden) {
    this.id = id;
    this.name = requireNonNull(name, "column name is null").getL();
    this.offset = offset;
    this.type = DataTypeFactory.of(requireNonNull(type, "type is null"));
    this.schemaState = SchemaState.fromValue(schemaState);
    this.comment = comment;
    this.defaultValue = defaultValue;
    this.originDefaultValue = originalDefaultValue;
    this.defaultValueBit = defaultValueBit;
    // I don't think pk flag should be set on type
    // Refactor against original tidb code
    this.isPrimaryKey = (type.getFlag() & PK_MASK) > 0;
    this.version = version;
    this.generatedExprString = generatedExprString;
    this.hidden = hidden;
  }

  public TiColumnInfo(
      long id,
      String name,
      int offset,
      DataType type,
      SchemaState schemaState,
      String originalDefaultValue,
      String defaultValue,
      String defaultValueBit,
      String comment,
      long version,
      String generatedExprString,
      boolean hidden) {
    this.id = id;
    this.name = requireNonNull(name, "column name is null").toLowerCase();
    this.offset = offset;
    this.type = requireNonNull(type, "data type is null");
    this.schemaState = schemaState;
    this.comment = comment;
    this.defaultValue = defaultValue;
    this.originDefaultValue = originalDefaultValue;
    this.defaultValueBit = defaultValueBit;
    this.isPrimaryKey = (type.getFlag() & PK_MASK) > 0;
    this.version = version;
    this.generatedExprString = generatedExprString;
    this.hidden = hidden;
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
    this.defaultValueBit = null;
    this.version = DataType.COLUMN_VERSION_FLAG;
    this.generatedExprString = "";
    this.hidden = false;
  }

  static TiColumnInfo getRowIdColumn(int offset) {
    return new TiColumnInfo(-1, "_tidb_rowid", offset, IntegerType.ROW_ID_TYPE, true);
  }

  TiColumnInfo copyWithoutPrimaryKey() {
    InternalTypeHolder typeHolder = type.toTypeHolder();
    typeHolder.setFlag(type.getFlag() & (~TiColumnInfo.PK_MASK));
    DataType newType = DataTypeFactory.of(typeHolder);
    return new TiColumnInfo(
        this.id,
        this.name,
        this.offset,
        newType,
        this.schemaState,
        this.originDefaultValue,
        this.defaultValue,
        this.defaultValueBit,
        this.comment,
        this.version,
        this.generatedExprString,
        this.hidden);
  }

  public long getId() {
    return this.id;
  }

  public String getName() {
    return this.name;
  }

  public boolean matchName(String name) {
    return this.name.equalsIgnoreCase(name)
        || String.format("`%s`", this.name).equalsIgnoreCase(name);
  }

  public int getOffset() {
    return this.offset;
  }

  public DataType getType() {
    return type;
  }

  public long getLength() {
    return getType().getLength();
  }

  public long getSize() {
    return getType().getSize();
  }

  SchemaState getSchemaState() {
    return schemaState;
  }

  public String getComment() {
    return comment;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  String getDefaultValue() {
    return defaultValue;
  }

  public String getDefaultValueBit() {
    return defaultValueBit;
  }

  // Default value use to stored in DefaultValue field, but now, bit type default value will store
  // in DefaultValueBit for fix bit default value decode/encode bug.
  String getOriginDefaultValue() {
    if (this.getType().getType() == MySQLType.TypeBit
        && originDefaultValue != null
        && defaultValueBit != null) {
      return defaultValueBit;
    } else {
      return originDefaultValue;
    }
  }

  private ByteString getOriginDefaultValueAsByteString() {
    CodecDataOutput cdo = new CodecDataOutput();
    type.encode(
        cdo, EncodeType.VALUE, type.getOriginDefaultValue(getOriginDefaultValue(), version));
    return cdo.toByteString();
  }

  public long getVersion() {
    return version;
  }

  public boolean isAutoIncrement() {
    return this.type.isAutoIncrement();
  }

  TiIndexColumn toFakeIndexColumn() {
    // we don't use original length of column since for a clustered index column
    // it always full index instead of prefix index
    return new TiIndexColumn(CIStr.newCIStr(getName()), getOffset(), DataType.UNSPECIFIED_LEN);
  }

  TiIndexColumn toIndexColumn() {
    return new TiIndexColumn(CIStr.newCIStr(getName()), getOffset(), getType().getLength());
  }

  ColumnInfo toProto(TiTableInfo tableInfo) {
    return ColumnInfo.newBuilder()
        .setColumnId(id)
        .setTp(type.getTypeCode())
        .setCollation(type.getCollationCode())
        .setColumnLen((int) type.getLength())
        .setDecimal(type.getDecimal())
        .setFlag(type.getFlag())
        .setDefaultVal(getOriginDefaultValueAsByteString())
        .setPkHandle(tableInfo.isPkHandle() && isPrimaryKey())
        .addAllElems(type.getElems())
        .build();
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    }

    if (!(other instanceof TiColumnInfo)) {
      return false;
    }

    TiColumnInfo col = (TiColumnInfo) other;
    return Objects.equals(id, col.id)
        && Objects.equals(name, col.name)
        && Objects.equals(type, col.type)
        && Objects.equals(schemaState, col.schemaState)
        && isPrimaryKey == col.isPrimaryKey
        && Objects.equals(defaultValue, col.defaultValue)
        && Objects.equals(originDefaultValue, col.originDefaultValue);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        id, name, type, schemaState, isPrimaryKey, defaultValue, originDefaultValue);
  }

  public String getGeneratedExprString() {
    return generatedExprString;
  }

  public boolean isGeneratedColumn() {
    return generatedExprString != null && !generatedExprString.isEmpty();
  }

  public boolean isHidden() {
    return hidden;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class InternalTypeHolder {
    private int tp;
    private int flag;
    private long flen;
    private int decimal;
    private String charset;
    private String collate;
    private List<String> elems;

    @JsonCreator
    public InternalTypeHolder(
        @JsonProperty("Tp") int tp,
        @JsonProperty("Flag") int flag,
        @JsonProperty("Flen") long flen,
        @JsonProperty("Decimal") int decimal,
        @JsonProperty("Charset") String charset,
        @JsonProperty("Collate") String collate,
        @JsonProperty("Elems") List<String> elems) {
      this.tp = tp;
      this.flag = flag;
      this.flen = flen;
      this.decimal = decimal;
      this.charset = charset;
      this.collate = collate;
      this.elems = elems;
    }

    public int getTp() {
      return tp;
    }

    public void setTp(int tp) {
      this.tp = tp;
    }

    public int getFlag() {
      return flag;
    }

    public void setFlag(int flag) {
      this.flag = flag;
    }

    public long getFlen() {
      return flen;
    }

    public void setFlen(long flen) {
      this.flen = flen;
    }

    public int getDecimal() {
      return decimal;
    }

    public void setDecimal(int decimal) {
      this.decimal = decimal;
    }

    public String getCharset() {
      return charset;
    }

    public void setCharset(String charset) {
      this.charset = charset;
    }

    public String getCollate() {
      return collate;
    }

    public void setCollate(String collate) {
      this.collate = collate;
    }

    public List<String> getElems() {
      return elems;
    }

    public void setElems(List<String> elems) {
      this.elems = elems;
    }

    interface Builder<E extends DataType> {
      E build(InternalTypeHolder holder);
    }
  }
}
