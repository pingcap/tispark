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
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableList;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.columnar.TiChunkColumnVector;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/** Base Type for encoding and decoding TiDB row information. */
public abstract class DataType implements Serializable {

  // Flag Information for strict mysql type
  public static final int NotNullFlag = 1; /* Field can't be NULL */
  public static final int PriKeyFlag = 2; /* Field is part of a primary key */
  public static final int UniqueKeyFlag = 4; /* Field is part of a unique key */
  public static final int MultipleKeyFlag = 8; /* Field is part of a key */
  public static final int BlobFlag = 16; /* Field is a blob */
  public static final int UnsignedFlag = 32; /* Field is unsigned */
  public static final int ZerofillFlag = 64; /* Field is zerofill */
  public static final int BinaryFlag = 128; /* Field is binary   */
  public static final int EnumFlag = 256; /* Field is an enum */
  public static final int AutoIncrementFlag = 512; /* Field is an auto increment field */
  public static final int TimestampFlag = 1024; /* Field is a timestamp */
  public static final int SetFlag = 2048; /* Field is a set */
  public static final int NoDefaultValueFlag = 4096; /* Field doesn't have a default value */
  public static final int OnUpdateNowFlag = 8192; /* Field is set to NOW on UPDATE */
  public static final int NumFlag = 32768; /* Field is a num (for clients) */
  public static final long COLUMN_VERSION_FLAG = 1;
  public static final int UNSPECIFIED_LEN = -1;
  private static final Long MaxInt8 = (1L << 7) - 1;
  private static final Long MinInt8 = -1L << 7;
  private static final Long MaxInt16 = (1L << 15) - 1;
  private static final Long MinInt16 = -1L << 15;
  private static final Long MaxInt24 = (1L << 23) - 1;
  private static final Long MinInt24 = -1L << 23;
  private static final Long MaxInt32 = (1L << 31) - 1;
  private static final Long MinInt32 = -1L << 31;
  private static final Long MaxInt64 = (1L << 63) - 1;
  private static final Long MinInt64 = -1L << 63;
  private static final Long MaxUint8 = (1L << 8) - 1;
  private static final Long MaxUint16 = (1L << 16) - 1;
  private static final Long MaxUint24 = (1L << 24) - 1;
  private static final Long MaxUint32 = (1L << 32) - 1;
  private static final Long MaxUint64 = -1L;
  // MySQL type
  protected final MySQLType tp;
  // Not Encode/Decode flag, this is used to strict mysql type
  // such as not null, timestamp
  protected final int flag;
  protected final int decimal;
  protected final int collation;
  protected final long length;
  private final String charset;
  private final List<String> elems;
  private final byte[] allNotNullBitMap = initAllNotNullBitMap();
  private final byte[] readBuffer = new byte[8];

  public DataType(MySQLType tp, int prec, int scale) {
    this.tp = tp;
    this.flag = 0;
    this.elems = ImmutableList.of();
    this.length = prec;
    this.decimal = scale;
    this.charset = "";
    this.collation = Collation.DEF_COLLATION_CODE;
  }

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

  protected DataType(
      MySQLType type, int flag, int len, int decimal, String charset, int collation) {
    this.tp = type;
    this.flag = flag;
    this.elems = ImmutableList.of();
    this.length = len;
    this.decimal = decimal;
    this.charset = charset;
    this.collation = collation;
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

  public static boolean isLengthUnSpecified(long length) {
    return length == UNSPECIFIED_LEN;
  }

  public Long signedLowerBound() throws TypeException {
    switch (this.getType()) {
      case TypeTiny:
        return MinInt8;
      case TypeShort:
        return MinInt16;
      case TypeInt24:
        return MinInt24;
      case TypeLong:
        return MinInt32;
      case TypeLonglong:
        return MinInt64;
      default:
        throw new TypeException("Signed Lower Bound: Input Type is not a mysql SIGNED type");
    }
  }

  public Long signedUpperBound() throws TypeException {
    switch (this.getType()) {
      case TypeTiny:
        return MaxInt8;
      case TypeShort:
        return MaxInt16;
      case TypeInt24:
        return MaxInt24;
      case TypeLong:
        return MaxInt32;
      case TypeLonglong:
        return MaxInt64;
      default:
        throw new TypeException("Signed Upper Bound: Input Type is not a mysql SIGNED type");
    }
  }

  public Long unsignedUpperBound() throws TypeException {
    switch (this.getType()) {
      case TypeTiny:
        return MaxUint8;
      case TypeShort:
        return MaxUint16;
      case TypeInt24:
        return MaxUint24;
      case TypeLong:
        return MaxUint32;
      case TypeLonglong:
      case TypeBit:
      case TypeEnum:
      case TypeSet:
        return MaxUint64;
      default:
        throw new TypeException("Unsigned Upper Bound: Input Type is not a mysql UNSIGNED type");
    }
  }

  protected abstract Object decodeNotNull(int flag, CodecDataInput cdi);

  protected Object decodeNotNullForBatchWrite(int flag, CodecDataInput cdi) {
    return decodeNotNull(flag, cdi);
  }

  private int getFixLen() {
    switch (this.getType()) {
      case TypeFloat:
        return 4;
      case TypeTiny:
      case TypeShort:
      case TypeInt24:
      case TypeLong:
      case TypeLonglong:
      case TypeDouble:
      case TypeYear:
      case TypeDuration:
      case TypeTimestamp:
      case TypeDate:
      case TypeDatetime:
        return 8;
      case TypeDecimal:
        throw new UnsupportedOperationException(
            "this should not get involved in calculation process");
      case TypeNewDecimal:
        return 40;
      default:
        return -1;
    }
  }

  private byte[] setAllNotNull(int numNullBitMapBytes) {
    byte[] nullBitMaps = new byte[numNullBitMapBytes];
    for (int i = 0; i < numNullBitMapBytes; ) {
      // allNotNullBitNMap's actual length
      int numAppendBytes = Math.min(numNullBitMapBytes - i, 128);
      if (numAppendBytes >= 0)
        System.arraycopy(allNotNullBitMap, 0, nullBitMaps, i, numAppendBytes);
      i += numAppendBytes;
    }
    return nullBitMaps;
  }

  private byte[] initAllNotNullBitMap() {
    byte[] allNotNullBitMap = new byte[128];
    Arrays.fill(allNotNullBitMap, (byte) 0xFF);
    return allNotNullBitMap;
  }

  private int readIntLittleEndian(CodecDataInput cdi) {
    int ch1 = cdi.readUnsignedByte();
    int ch2 = cdi.readUnsignedByte();
    int ch3 = cdi.readUnsignedByte();
    int ch4 = cdi.readUnsignedByte();
    return ((ch1) + (ch2 << 8) + (ch3 << 16) + (ch4 << 24));
  }

  private long readLongLittleEndian(CodecDataInput cdi) {
    cdi.readFully(readBuffer, 0, 8);
    return ((readBuffer[0] & 255)
        + ((readBuffer[1] & 255) << 8)
        + ((readBuffer[2] & 255) << 16)
        + ((readBuffer[3] & 255) << 24)
        + ((long) (readBuffer[4] & 255) << 32)
        + ((long) (readBuffer[5] & 255) << 40)
        + ((long) (readBuffer[6] & 255) << 48)
        + ((long) (readBuffer[7] & 255) << 56));
  }

  public boolean isSameCatalog(DataType other) {
    return false;
  }

  // all data should be read in little endian.
  public TiChunkColumnVector decodeChunkColumn(CodecDataInput cdi) {
    int numRows = readIntLittleEndian(cdi);
    int numNulls = readIntLittleEndian(cdi);
    assert (numRows >= 0) && (numNulls >= 0);
    int numNullBitmapBytes = (numRows + 7) / 8;
    byte[] nullBitMaps = new byte[numNullBitmapBytes];
    if (numNulls > 0) {
      cdi.readFully(nullBitMaps);
    } else {
      nullBitMaps = setAllNotNull(numNullBitmapBytes);
    }

    int numFixedBytes = getFixLen();
    int numDataBytes = numFixedBytes * numRows;

    int numOffsets;
    long[] offsets = null;
    // handle var element
    if (numFixedBytes == -1) {
      numOffsets = numRows + 1;
      // read numOffsets * 8 bytes array
      // and convert bytes to int64
      offsets = new long[numOffsets];
      for (int i = 0; i < numOffsets; i++) {
        offsets[i] = readLongLittleEndian(cdi);
      }
      numDataBytes = (int) offsets[numRows];
    }

    // TODO this costs a lot, we need to find a way to avoid.
    byte[] dataBuffer = new byte[numDataBytes];
    cdi.readFully(dataBuffer);
    ByteBuffer buffer = ByteBuffer.wrap(dataBuffer);
    buffer.order(LITTLE_ENDIAN);
    return new TiChunkColumnVector(
        this, numFixedBytes, numRows, numNulls, nullBitMaps, offsets, buffer);
  }

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

  public Object decodeForBatchWrite(CodecDataInput cdi) {
    int flag = cdi.readUnsignedByte();
    if (isNullFlag(flag)) {
      return null;
    }
    return decodeNotNullForBatchWrite(flag, cdi);
  }

  public boolean isNextNull(CodecDataInput cdi) {
    return isNullFlag(cdi.peekByte());
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

  /**
   * Convert from Spark SQL Supported Java Type to TiDB Type
   *
   * <p>1. data convert, e.g. Integer -> SHORT
   *
   * <p>2. check overflow, e.g. write 1000 to short
   *
   * <p>Spark SQL only support following types:
   *
   * <p>1. BooleanType -> java.lang.Boolean 2. ByteType -> java.lang.Byte 3. ShortType ->
   * java.lang.Short 4. IntegerType -> java.lang.Integer 5. LongType -> java.lang.Long 6. FloatType
   * -> java.lang.Float 7. DoubleType -> java.lang.Double 8. StringType -> String 9. DecimalType ->
   * java.math.BigDecimal 10. DateType -> java.sql.Date 11. TimestampType -> java.sql.Timestamp 12.
   * BinaryType -> byte array 13. ArrayType -> scala.collection.Seq (use getList for java.util.List)
   * 14. MapType -> scala.collection.Map (use getJavaMap for java.util.Map) 15. StructType ->
   * org.apache.spark.sql.Row
   *
   * @param value
   * @return
   * @throws ConvertNotSupportException
   * @throws ConvertOverflowException
   */
  public Object convertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    if (value == null) {
      return null;
    } else {
      return doConvertToTiDBType(value);
    }
  }

  protected abstract Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException;

  protected abstract void encodeKey(CodecDataOutput cdo, Object value);

  protected abstract void encodeValue(CodecDataOutput cdo, Object value);

  protected abstract void encodeProto(CodecDataOutput cdo, Object value);

  public abstract String getName();

  /**
   * encode a Key's prefix to CodecDataOutput
   *
   * @param cdo destination of data.
   * @param value value to be encoded.
   * @param prefixLength specifies prefix length of value to be encoded. When prefixLength is
   *     DataType.UNSPECIFIED_LEN, encode full length of value.
   */
  public void encodeKey(CodecDataOutput cdo, Object value, int prefixLength) {
    requireNonNull(cdo, "cdo is null");
    if (value == null) {
      encodeNull(cdo);
    } else if (DataType.isLengthUnSpecified(prefixLength)) {
      encodeKey(cdo, value);
    } else if (isPrefixIndexSupported()) {
      byte[] bytes;
      // When charset is utf8/utf8mb4, prefix length should be the number of utf8 characters
      // rather than length of its encoded byte value.
      if (getCharset().equalsIgnoreCase("utf8") || getCharset().equalsIgnoreCase("utf8mb4")) {
        bytes = Converter.convertUtf8ToBytes(value, prefixLength);
      } else {
        bytes = Converter.convertToBytes(value, prefixLength);
      }
      BytesCodec.writeBytesFully(cdo, bytes);
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
   *
   * @param value a int value represents in string
   * @return a int object
   */
  public abstract Object getOriginDefaultValueNonNull(String value, long version);

  /** @return true if this type can be pushed down to TiKV or TiFLASH */
  public boolean isPushDownSupported() {
    return true;
  }

  public Object getOriginDefaultValue(String value, long version) {
    if (value == null) return null;
    return getOriginDefaultValueNonNull(value, version);
  }

  public int getCollationCode() {
    return collation;
  }

  public long getLength() {
    return length;
  }

  long getDefaultDataSize() {
    return tp.getDefaultSize();
  }

  long getPrefixSize() {
    return tp.getPrefixSize();
  }

  public int getDefaultLength() {
    return tp.getDefaultLength();
  }

  /**
   * Size of data type
   *
   * @return size
   */
  public long getSize() {
    // TiDB types are prepended with a type flag.
    return getPrefixSize() + getDefaultDataSize();
  }

  public boolean isLengthUnSpecified() {
    return DataType.isLengthUnSpecified(length);
  }

  public boolean isDecimalUnSpecified() {
    return DataType.isLengthUnSpecified(decimal);
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

  public boolean isUnsigned() {
    return (flag & UnsignedFlag) > 0;
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
    return (int)
        (31
            * (tp.getTypeCode() == 0 ? 1 : tp.getTypeCode())
            * (flag == 0 ? 1 : flag)
            * (decimal == 0 ? 1 : decimal)
            * (charset == null ? 1 : charset.hashCode())
            * (collation == 0 ? 1 : collation)
            * (length == 0 ? 1 : length)
            * (elems.hashCode()));
  }

  public InternalTypeHolder toTypeHolder() {
    return new InternalTypeHolder(
        getTypeCode(), flag, length, decimal, charset, Collation.translate(collation), elems);
  }

  public enum EncodeType {
    KEY,
    VALUE,
    PROTO
  }
}
