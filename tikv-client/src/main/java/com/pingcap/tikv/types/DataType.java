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

package com.pingcap.tikv.types;

import static com.pingcap.tikv.types.Types.AutoIncrementFlag;
import static com.pingcap.tikv.types.Types.MultipleKeyFlag;
import static com.pingcap.tikv.types.Types.NoDefaultValueFlag;
import static com.pingcap.tikv.types.Types.NotNullFlag;
import static com.pingcap.tikv.types.Types.OnUpdateNowFlag;
import static com.pingcap.tikv.types.Types.PriKeyFlag;
import static com.pingcap.tikv.types.Types.TimestampFlag;
import static com.pingcap.tikv.types.Types.UniqueKeyFlag;
import static com.pingcap.tikv.types.Types.UnsignedFlag;
import static com.pingcap.tikv.types.Types.ZerofillFlag;

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.row.Row;
import java.io.Serializable;
import java.util.List;

/** Base Type for encoding and decoding TiDB row information. */
public abstract class DataType implements Serializable {
  public enum EncodeType {
    KEY,
    VALUE
  }

  public static final int UNSPECIFIED_LEN = -1;

  // encoding/decoding flag
  private static final int NULL_FLAG = 0;
  public static final int BYTES_FLAG = 1;
  public static final int COMPACT_BYTES_FLAG = 2;
  public static final int INT_FLAG = 3;
  public static final int UINT_FLAG = 4;
  public static final int FLOATING_FLAG = 5;
  public static final int DECIMAL_FLAG = 6;
  public static final int DURATION_FLAG = 7;
  public static final int VARINT_FLAG = 8;
  public static final int UVARINT_FLAG = 9;
  public static final int JSON_FLAG = 10;
  public static final int MAX_FLAG = 250;
  // MySQL type
  protected int tp;
  // Not Encode/Decode flag, this is used to strict mysql type
  // such as not null, timestamp
  protected int flag;
  protected int decimal;
  protected int collation;
  protected long length;
  private List<String> elems;

  protected DataType(TiColumnInfo.InternalTypeHolder holder) {
    this.tp = holder.getTp();
    this.flag = holder.getFlag();
    this.length = holder.getFlen();
    this.decimal = holder.getDecimal();
    this.collation = Collation.translate(holder.getCollate());
    this.elems = holder.getElems() == null ? ImmutableList.of() : holder.getElems();
  }

  protected DataType() {
    this.flag = 0;
    this.elems = ImmutableList.of();
    this.length = UNSPECIFIED_LEN;
    this.collation = Collation.DEF_COLLATION_CODE;
  }

  protected DataType(int tp) {
    this.tp = tp;
    this.flag = 0;
    this.elems = ImmutableList.of();
    this.length = UNSPECIFIED_LEN;
    this.decimal = UNSPECIFIED_LEN;
    this.collation = Collation.DEF_COLLATION_CODE;
  }

  protected DataType(int flag, int length, String collation, List<String> elems, int tp) {
    this.tp = tp;
    this.flag = flag;
    this.length = length;
    this.collation = Collation.translate(collation);
    this.elems = elems == null ? ImmutableList.of() : elems;
    this.tp = tp;
  }

  protected boolean isNullFlag(int flag) {
    return flag == NULL_FLAG;
  }

  protected void decodeValueNoNullToRow(Row row, int pos, Object value) {
    row.set(pos, this, value);
  }

  public abstract Object decodeNotNull(int flag, CodecDataInput cdi);

  /**
   * decode value from row which is nothing.
   *
   * @param cdi source of data.
   */
  public Object decode(CodecDataInput cdi) {
    int flag = cdi.readUnsignedByte();
    if (isNullFlag(flag)) {
      return null;
    }
    return decodeNotNull(flag, cdi);
  }

  public void decodeValueToRow(CodecDataInput cdi, Row row, int pos) {
    int flag = cdi.readUnsignedByte();
    if (isNullFlag(flag)) {
      row.setNull(pos);
    } else {
      decodeValueNoNullToRow(row, pos, decodeNotNull(flag, cdi));
    }
  }

  public static void encodeIndexMaxValue(CodecDataOutput cdo) {
    cdo.writeByte(MAX_FLAG);
  }

  public static void encodeIndexMinValue(CodecDataOutput cdo) {
    cdo.writeByte(BYTES_FLAG);
  }

  public static int indexMinValueFlag() {
    return BYTES_FLAG;
  }

  public static int indexMaxValueFlag() {
    return MAX_FLAG;
  }

  public static ByteString encodeIndexMaxValue() {
    CodecDataOutput cdo = new CodecDataOutput();
    encodeIndexMaxValue(cdo);
    return cdo.toByteString();
  }

  public static ByteString encodeIndexMinValue() {
    CodecDataOutput cdo = new CodecDataOutput();
    encodeIndexMinValue(cdo);
    return cdo.toByteString();
  }

  /**
   * encode max value.
   *
   * @param cdo destination of data.
   */
  public void encodeMaxValue(CodecDataOutput cdo) {
    encodeIndexMaxValue(cdo);
  }

  /**
   * encode min value.
   *
   * @param cdo destination of data.
   */
  public void encodeMinValue(CodecDataOutput cdo) {
    encodeIndexMinValue(cdo);
  }

  /**
   * encode a Row to CodecDataOutput
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  public void encode(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    if (value == null) {
      cdo.writeByte(NULL_FLAG);
    } else {
      encodeNotNull(cdo, encodeType, value);
    }
  }

  public abstract void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value);
  public abstract Object getOriginDefaultValueNonNull(String value);

  public Object getOriginDefaultValue(String value) {
    if(value == null) return null;
    return getOriginDefaultValueNonNull(value);
  }

  public int getCollationCode() {
    return collation;
  }

  public long getLength() {
    return length;
  }

  public int getDecimal() {
    return decimal;
  }

  public void setFlag(int flag) {
    this.flag = flag;
  }

  public int getFlag() {
    return flag;
  }

  public List<String> getElems() {
    return this.elems;
  }

  public int getTypeCode() {
    return tp;
  }

  public static boolean hasNullFlag(int flag) {
    return (flag & NotNullFlag) > 0;
  }

  public static boolean hasNoDefaultFlag(int flag) {
    return (flag & NoDefaultValueFlag) > 0;
  }

  public static boolean hasAutoIncrementFlag(int flag) {
    return (flag & AutoIncrementFlag) > 0;
  }

  public static boolean hasUnsignedFlag(int flag) {
    return (flag & UnsignedFlag) > 0;
  }

  public static boolean hasZerofillFlag(int flag) {
    return (flag & ZerofillFlag) > 0;
  }

  public static boolean hasBinaryFlag(int flag) {
    return (flag & PriKeyFlag) > 0;
  }

  public static boolean hasUniKeyFlag(int flag) {
    return (flag & UniqueKeyFlag) > 0;
  }

  public static boolean hasMultipleKeyFlag(int flag) {
    return (flag & MultipleKeyFlag) > 0;
  }

  public static boolean hasTimestampFlag(int flag) {
    return (flag & TimestampFlag) > 0;
  }

  public static boolean hasOnUpdateNowFlag(int flag) {
    return (flag & OnUpdateNowFlag) > 0;
  }

  public boolean needCast(Object val) {
    // TODO: Add implementations
    return false;
  }

  @Override
  public String toString() {
    return this.getClass().getSimpleName();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DataType) {
      DataType otherType = (DataType) other;
      // tp implies Class is the same
      // and that might not always hold
      // TODO: reconsider design here
      return tp == otherType.tp
          && flag == otherType.flag
          && decimal == otherType.decimal
          && collation == otherType.collation
          && length == otherType.length
          && elems.equals(otherType.elems);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) (31
            * (tp == 0 ? 1 : tp)
            * (flag == 0 ? 1 : flag)
            * (decimal == 0 ? 1 : decimal)
            * (collation == 0 ? 1 : collation)
            * (length == 0 ? 1 : length)
            * (elems.hashCode()));
  }
}
