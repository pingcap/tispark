/*
 * Copyright 2020 PingCAP, Inc.
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

package org.tikv.common.columnar;

import static org.tikv.common.util.MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT;

import org.tikv.common.ExtendedDateTime;
import org.tikv.common.codec.Codec.DateCodec;
import org.tikv.common.codec.Codec.DateTimeCodec;
import org.tikv.common.columnar.datatypes.CHType;
import org.tikv.common.types.AbstractDateTimeType;
import org.tikv.common.types.BytesType;
import org.tikv.common.types.DateType;
import org.tikv.common.util.MemoryUtil;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

public class TiBlockColumnVector extends TiColumnVector {
  long offsetsAddr;
  ByteBuffer offsets;
  long nullMapAddr;
  ByteBuffer nullMap;
  long dataAddr;
  ByteBuffer data;
  private int fixedLength;

  public TiBlockColumnVector(CHType type, ByteBuffer data, int numOfRows, int fixedLength) {
    super(type.toDataType(), numOfRows);
    this.data = data;
    this.dataAddr = MemoryUtil.getAddress(data);
    fillEmptyNullMap();
    fillEmptyOffsets();
    this.fixedLength = fixedLength;
  }

  public TiBlockColumnVector(CHType type) {
    super(type.toDataType(), 0);
  }

  public TiBlockColumnVector(
      CHType type, ByteBuffer nullMap, ByteBuffer data, int numOfRows, int fixedLength) {
    // chType -> data type
    super(type.toDataType(), numOfRows);
    this.nullMap = nullMap;
    this.nullMapAddr = MemoryUtil.getAddress(nullMap);
    this.data = data;
    this.dataAddr = MemoryUtil.getAddress(data);
    fillEmptyOffsets();
    this.fixedLength = fixedLength;
  }

  /** Sets up the data type of this column vector. */
  public TiBlockColumnVector(
      CHType type, ByteBuffer nullMap, ByteBuffer offsets, ByteBuffer data, int numOfRows) {
    // chType -> data type
    super(type.toDataType(), numOfRows);
    this.offsets = offsets;
    this.offsetsAddr = MemoryUtil.getAddress(offsets);
    this.nullMap = nullMap;
    this.nullMapAddr = MemoryUtil.getAddress(nullMap);
    this.data = data;
    this.dataAddr = MemoryUtil.getAddress(data);
    this.fixedLength = -1;
  }

  private void fillEmptyNullMap() {
    this.nullMap = EMPTY_BYTE_BUFFER_DIRECT;
    this.nullMapAddr = MemoryUtil.getAddress(this.nullMap);
  }

  private void fillEmptyOffsets() {
    this.offsets = EMPTY_BYTE_BUFFER_DIRECT;
    this.offsetsAddr = MemoryUtil.getAddress(this.offsets);
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  @Override
  public void close() {
    if (dataAddr != 0) {
      MemoryUtil.free(data);
    }

    if (offsetsAddr != 0) {
      MemoryUtil.free(offsets);
    }

    if (nullMapAddr != 0) {
      MemoryUtil.free(nullMap);
    }
    dataAddr = 0;
    offsetsAddr = 0;
    nullMapAddr = 0;
  }

  /** Returns true if this column vector contains any null values. */
  @Override
  public boolean hasNull() {
    return nullMap == null;
  }

  /** Returns the number of nulls in this column vector. */
  @Override
  public int numNulls() {
    throw new UnsupportedOperationException("numNulls is not supported for TiBlockColumnVector");
  }

  /** Returns whether the value at rowId is NULL. */
  @Override
  public boolean isNullAt(int rowId) {
    if (nullMap == EMPTY_BYTE_BUFFER_DIRECT) {
      return false;
    }
    return MemoryUtil.getByte(nullMapAddr + rowId) != 0;
  }

  /**
   * Returns the boolean type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public boolean getBoolean(int rowId) {
    return false;
  }

  /**
   * Returns the byte type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public byte getByte(int rowId) {
    return MemoryUtil.getByte(dataAddr + rowId);
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public short getShort(int rowId) {
    return MemoryUtil.getShort(dataAddr + ((long) rowId << 1));
  }

  /**
   * Returns the int type value for rowId. The return value is undefined and can be anything, if the
   * slot for rowId is null.
   */
  @Override
  public int getInt(int rowId) {
    if (type instanceof DateType) {
      return (int) getTime(rowId);
    }
    return MemoryUtil.getInt(dataAddr + ((long) rowId << 2));
  }

  private long getDateTime(int rowId, DateTimeZone tz) {
    long v = MemoryUtil.getLong(dataAddr + ((long) rowId << 3));
    ExtendedDateTime extendedDateTime = DateTimeCodec.fromPackedLong(v, tz);
    if (extendedDateTime == null) {
      // TODO: make this behavior configurable
      return 0;
    }
    Timestamp ts = extendedDateTime.toTimeStamp();
    return ts.getTime() / 1000 * 1000000 + ts.getNanos() / 1000;
  }

  private long getTime(int rowId) {
    long v = MemoryUtil.getLong(dataAddr + ((long) rowId << 3));
    LocalDate date = DateCodec.fromPackedLong(v);
    return ((DateType) type).getDays(date);
  }
  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public long getLong(int rowId) {
    if (type instanceof AbstractDateTimeType) {
      return getDateTime(rowId, ((AbstractDateTimeType) type).getTimezone());
    }
    if (fixedLength == 1) {
      return getByte(rowId);
    } else if (fixedLength == 2) {
      return getShort(rowId);
    } else if (fixedLength == 4) {
      return getInt(rowId);
    } else if (fixedLength == 8) {
      return MemoryUtil.getLong(dataAddr + ((long) rowId * fixedLength));
    }
    throw new UnsupportedOperationException(
        String.format("getting long with fixed length %d", fixedLength));
  }

  /**
   * Returns the float type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public float getFloat(int rowId) {
    return MemoryUtil.getFloat(dataAddr + ((long) rowId * fixedLength));
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public double getDouble(int rowId) {
    return MemoryUtil.getDouble(dataAddr + ((long) rowId * fixedLength));
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    long rowIdAddr = (long) rowId * fixedLength + dataAddr;
    if (fixedLength == 4) {
      return MemoryUtil.getDecimal32(rowIdAddr, scale);
    } else if (fixedLength == 8) {
      return MemoryUtil.getDecimal64(rowIdAddr, scale);
    } else if (fixedLength == 16) {
      return MemoryUtil.getDecimal128(rowIdAddr, scale);
    } else {
      return MemoryUtil.getDecimal256(rowIdAddr, scale);
    }
  }

  private long offsetAt(int i) {
    return i == 0 ? 0 : MemoryUtil.getLong(offsetsAddr + ((long) (i - 1) << 3));
  }

  public int sizeAt(int i) {
    return (int)
        (i == 0
            ? MemoryUtil.getLong(offsetsAddr)
            : MemoryUtil.getLong(offsetsAddr + ((long) i << 3))
                - MemoryUtil.getLong(offsetsAddr + ((long) (i - 1) << 3)));
  }

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  @Override
  public String getUTF8String(int rowId) {
    // FixedString case
    if (fixedLength != -1) {
      byte[] chars = new byte[fixedLength];
      MemoryUtil.getBytes((int) (dataAddr + fixedLength * rowId), chars, 0, fixedLength);
      return new String(chars);
    } else {
      long offset = (dataAddr + offsetAt(rowId));
      int numBytes = sizeAt(rowId) - 1;
      byte[] chars = new byte[numBytes];
      MemoryUtil.getBytes(offset, chars, 0, numBytes);
      return new String(chars, StandardCharsets.UTF_8);
    }
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    if (type.equals(BytesType.BLOB) || type.equals(BytesType.TINY_BLOB)) {
      long offset = (dataAddr + offsetAt(rowId));
      int numBytes = sizeAt(rowId) - 1;
      byte[] ret = new byte[numBytes];
      MemoryUtil.getBytes(offset, ret, 0, numBytes);
      return ret;
    } else {
      throw new UnsupportedOperationException(
          "get Binary for TiBlockColumnVector is not supported");
    }
  }

  /** @return child [[TiColumnVector]] at the given ordinal. */
  @Override
  protected TiColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("getChild is not supported for TiBlockColumnVector");
  }
}
