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

import static com.pingcap.tikv.codec.Codec.isNullFlag;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** Base Type for encoding and decoding TiDB row information. */
public abstract class DataType implements Serializable {

  // Flag Information for strict mysql type
  protected static final int NotNullFlag = 1; /* Field can't be NULL */
  protected static final int PriKeyFlag = 2; /* Field is part of a primary key */
  protected static final int UniqueKeyFlag = 4; /* Field is part of a unique key */
  protected static final int MultipleKeyFlag = 8; /* Field is part of a key */
  protected static final int BlobFlag = 16; /* Field is a blob */
  protected static final int UnsignedFlag = 32; /* Field is unsigned */
  protected static final int ZerofillFlag = 64; /* Field is zerofill */
  protected static final int BinaryFlag = 128; /* Field is binary   */
  protected static final int EnumFlag = 256; /* Field is an enum */
  protected static final int AutoIncrementFlag = 512; /* Field is an auto increment field */
  protected static final int TimestampFlag = 1024; /* Field is a timestamp */
  protected static final int SetFlag = 2048; /* Field is a set */
  protected static final int NoDefaultValueFlag = 4096; /* Field doesn't have a default value */
  protected static final int OnUpdateNowFlag = 8192; /* Field is set to NOW on UPDATE */
  protected static final int NumFlag = 32768; /* Field is a num (for clients) */

  public enum EncodeType {
    KEY,
    VALUE,
    PROTO
  }

  public static final int UNSPECIFIED_LEN = -1;

  // MySQL type
  protected final MySQLType tp;
  // Not Encode/Decode flag, this is used to strict mysql type
  // such as not null, timestamp
  protected final int flag;
  protected final int decimal;
  private final String charset;
  protected final int collation;
  protected final long length;
  private final List<String> elems;

  protected DataType(TiColumnInfo.InternalTypeHolder holder) {
    this.tp = MySQLType.fromTypeCode(holder.getTp());
    this.flag = holder.getFlag();
    this.length = holder.getFlen();
    this.decimal = holder.getDecimal();
    this.charset = holder.getCharset();
    this.collation = Collation.translate(holder.getCollate());
    this.elems = holder.getElems() == null ? ImmutableList.of() : holder.getElems();
  }

  protected DataType(MySQLType type) {
    this.tp = type;
    this.flag = 0;
    this.elems = ImmutableList.of();
    this.length = UNSPECIFIED_LEN;
    this.decimal = UNSPECIFIED_LEN;
    this.charset = "";
    this.collation = Collation.DEF_COLLATION_CODE;
  }

  protected DataType(MySQLType type, int flag, int len, int decimal, String charset, int collation) {
    this.tp = type;
    this.flag = flag;
    this.elems = ImmutableList.of();
    this.length = len;
    this.decimal = decimal;
    this.charset = charset;
    this.collation = collation;
  }

  protected abstract Object decodeNotNull(int flag, CodecDataInput cdi);

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

  public boolean isNextNull(CodecDataInput cdi) {
    return isNullFlag(cdi.peekByte());
  }

  public static void encodeMaxValue(CodecDataOutput cdo) {
    cdo.writeByte(Codec.MAX_FLAG);
  }

  public static void encodeNull(CodecDataOutput cdo) {
    cdo.writeByte(Codec.NULL_FLAG);
  }

  public static void encodeIndex(CodecDataOutput cdo) {
    cdo.writeByte(Codec.BYTES_FLAG);
  }

  /**
   * encode a Row to CodecDataOutput
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value value to be encoded.
   */
  public void encode(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    requireNonNull(cdo, "cdo is null");
    if (value == null) {
      if (encodeType != EncodeType.PROTO) {
        encodeNull(cdo);
      }
    } else {
      switch (encodeType) {
        case KEY:
          encodeKey(cdo, value);
          return;
        case VALUE:
          encodeValue(cdo, value);
          return;
        case PROTO:
          encodeProto(cdo, value);
          return;
        default:
          throw new TypeException("Unknown encoding type " + encodeType);
      }
    }
  }

  protected abstract void encodeKey(CodecDataOutput cdo, Object value);
  protected abstract void encodeValue(CodecDataOutput cdo, Object value);
  protected abstract void encodeProto(CodecDataOutput cdo, Object value);

  /**
   * encode a Key's prefix to CodecDataOutput
   *
   * @param cdo destination of data.
   * @param value value to be encoded.
   * @param prefixLength specifies prefix length of value to be encoded.
   *                     When prefixLength is DataType.UNSPECIFIED_LEN,
   *                     encode full length of value.
   */
  public void encodeKey(CodecDataOutput cdo, Object value, int prefixLength) {
    requireNonNull(cdo, "cdo is null");
    if (value == null) {
      encodeNull(cdo);
    } else if (prefixLength == DataType.UNSPECIFIED_LEN) {
      encodeKey(cdo, value);
    } else if (isPrefixIndexSupported()) {
        byte[] bytes = Converter.convertToBytes(value);
        Codec.BytesCodec.writeBytesFully(cdo, Arrays.copyOf(bytes, prefixLength));
    } else {
      throw new TypeException("Data type can not encode with prefix");
    }
  }

  /**
   * Indicates whether a data type supports prefix index
   *
   * @return returns true iff the type is BytesType
   */
  protected boolean isPrefixIndexSupported() {
    return false;
  }

  public abstract ExprType getProtoExprType();

  /**
   * get origin default value
   * @param value a int value represents in string
   * @return a int object
   */
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

  public int getFlag() {
    return flag;
  }

  public List<String> getElems() {
    return this.elems;
  }

  public int getTypeCode() {
    return tp.getTypeCode();
  }

  public MySQLType getType() {
    return tp;
  }

  public String getCharset() {
    return charset;
  }

  public boolean isPrimaryKey() {
    return (flag & PriKeyFlag) > 0;
  }

  public boolean isNotNull() {
    return (flag & NotNullFlag) > 0;
  }

  public boolean isNoDefault() {
    return (flag & NoDefaultValueFlag) > 0;
  }

  public boolean isAutoIncrement() {
    return (flag & AutoIncrementFlag) > 0;
  }

  public boolean isZeroFill() {
    return (flag & ZerofillFlag) > 0;
  }

  public boolean isBinary() {
    return (flag & BinaryFlag) > 0;
  }

  public boolean isUniqueKey() {
    return (flag & UniqueKeyFlag) > 0;
  }

  public boolean isMultiKey() {
    return (flag & MultipleKeyFlag) > 0;
  }

  public boolean isTimestamp() {
    return (flag & TimestampFlag) > 0;
  }

  public boolean isOnUpdateNow() {
    return (flag & OnUpdateNowFlag) > 0;
  }

  public boolean isBlob() {
    return (flag & BlobFlag) > 0;
  }

  public boolean isEnum() {
    return (flag & EnumFlag) > 0;
  }

  public boolean isSet() {
    return (flag & SetFlag) > 0;
  }

  public boolean isNum() {
    return (flag & NumFlag) > 0;
  }

  @Override
  public String toString() {
    return String.format("%s:%s", this.getClass().getSimpleName(), getType());
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
          && (charset != null && charset.equals(otherType.charset))
          && collation == otherType.collation
          && length == otherType.length
          && elems.equals(otherType.elems);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return (int) (31
            * (tp.getTypeCode() == 0 ? 1 : tp.getTypeCode())
            * (flag == 0 ? 1 : flag)
            * (decimal == 0 ? 1 : decimal)
            * (charset == null ? 1 : charset.hashCode())
            * (collation == 0 ? 1 : collation)
            * (length == 0 ? 1 : length)
            * (elems.hashCode()));
  }
}
