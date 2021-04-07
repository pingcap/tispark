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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.columnar;

import static com.pingcap.tikv.util.MemoryUtil.EMPTY_BYTE_BUFFER;
import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.columnar.datatypes.CHType;
import com.pingcap.tikv.types.AbstractDateTimeType;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.util.MemoryUtil;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.joda.time.LocalDate;

public class TiBlockColumnVector extends TiColumnVector {
  private final ByteBuffer offsets;
  private final ByteBuffer nullMap;
  private final ByteBuffer data;
  private final int fixedLength;

  public TiBlockColumnVector(CHType type, ByteBuffer data, int numOfRows, int fixedLength) {
    super(type.toDataType(), numOfRows);
    this.data = requireNonNull(data);
    this.nullMap = null;
    this.offsets = null;
    this.fixedLength = fixedLength;
  }

  public TiBlockColumnVector(CHType type) {
    super(type.toDataType(), 0);
    this.data = EMPTY_BYTE_BUFFER;
    this.nullMap = null;
    this.offsets = null;
    this.fixedLength = -1;
  }

  public TiBlockColumnVector(
      CHType type, ByteBuffer nullMap, ByteBuffer data, int numOfRows, int fixedLength) {
    // chType -> data type
    super(type.toDataType(), numOfRows);
    this.nullMap = requireNonNull(nullMap);
    this.data = requireNonNull(data);
    this.offsets = null;
    this.fixedLength = fixedLength;
  }

  /** Sets up the data type of this column vector. */
  public TiBlockColumnVector(
      CHType type, ByteBuffer nullMap, ByteBuffer offsets, ByteBuffer data, int numOfRows) {
    // chType -> data type
    super(type.toDataType(), numOfRows);
    this.offsets = requireNonNull(offsets);
    this.nullMap = nullMap; // may be null
    this.data = requireNonNull(data);
    this.fixedLength = -1;
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  @Override
  public void close() {}

  /** Returns true if this column vector contains any null values. */
  @Override
  public boolean hasNull() {
    if (nullMap != null) {
      byte[] array = nullMap.array();
      for (byte b : array) {
        if (b != 0) return true;
      }
    }
    return false;
  }

  /** Returns the number of nulls in this column vector. */
  @Override
  public int numNulls() {
    int n = 0;
    if (nullMap != null) {
      byte[] array = nullMap.array();
      for (byte b : array) {
        if (b != 0) n++;
      }
    }
    return n;
  }

  /** Returns whether the value at rowId is NULL. */
  @Override
  public boolean isNullAt(int rowId) {
    return nullMap != null && nullMap.get(rowId) != 0;
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
    return data.get(rowId);
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public short getShort(int rowId) {
    return data.getShort(rowId << 1);
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
    return data.getInt(rowId << 2);
  }

  // returns microseconds since epoch - fields are interpreted in current default timezone
  private long getDateTime(int rowId) {
    long v = data.getLong(rowId << 3);
    long ymdhms = v >>> 24;
    long ymd = ymdhms >>> 17;
    int day = (int) (ymd & ((1 << 5) - 1));
    long ym = ymd >>> 5;
    int month = (int) (ym % 13);
    int year = (int) (ym / 13);

    int hms = (int) (ymdhms & ((1 << 17) - 1));
    int second = hms & ((1 << 6) - 1);
    int minute = (hms >>> 6) & ((1 << 6) - 1);
    int hour = hms >>> 12;
    int microsec = (int) (v % (1 << 24));
    ZonedDateTime zdt =
        ZonedDateTime.of(
            year, month, day, hour, minute, second, microsec * 1000, ZoneId.systemDefault());
    Instant instant = zdt.toInstant();
    return instant.getEpochSecond() * 1000_000L + instant.getNano() / 1000;
  }

  private long getTime(int rowId) {
    long v = data.getLong(rowId << 3);
    long ymd = v >>> 41;
    long ym = ymd >>> 5;
    int year = (int) (ym / 13);
    int month = (int) (ym % 13);
    int day = (int) (ymd & ((1 << 5) - 1));
    LocalDate date = new LocalDate(year, month, day);
    return ((DateType) type).getDays(date);
  }
  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public long getLong(int rowId) {
    if (type instanceof AbstractDateTimeType) {
      return getDateTime(rowId);
    }
    if (fixedLength == 1) {
      return getByte(rowId);
    } else if (fixedLength == 2) {
      return getShort(rowId);
    } else if (fixedLength == 4) {
      return getInt(rowId);
    } else if (fixedLength == 8) {
      return data.getLong(rowId << 3);
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
    return data.getFloat(rowId * fixedLength);
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public double getDouble(int rowId) {
    return data.getDouble(rowId * fixedLength);
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    if (fixedLength == 4) {
      return MemoryUtil.getDecimal32(data, rowId << 2, scale);
    } else if (fixedLength == 8) {
      return MemoryUtil.getDecimal64(data, rowId << 3, scale);
    } else if (fixedLength == 16) {
      return MemoryUtil.getDecimal128(data, rowId << 4, scale);
    } else {
      return MemoryUtil.getDecimal256(data, rowId * fixedLength, scale);
    }
  }

  private long offsetAt(int i) {
    return i == 0 ? 0L : offsets.getLong((i - 1) << 3);
  }

  public int sizeAt(int i) {
    return (int)
        (i == 0 ? offsets.getLong(0) : offsets.getLong(i << 3) - offsets.getLong((i - 1) << 3));
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
      data.get(chars, rowId * fixedLength, fixedLength);
      return new String(chars);
    } else {
      int offset = (int) offsetAt(rowId);
      int numBytes = sizeAt(rowId) - 1;
      byte[] chars = new byte[numBytes];
      data.get(chars, offset, numBytes);
      return new String(chars, StandardCharsets.UTF_8);
    }
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    if (type.equals(BytesType.BLOB) || type.equals(BytesType.TINY_BLOB)) {
      int offset = (int) offsetAt(rowId);
      int numBytes = sizeAt(rowId) - 1;
      byte[] ret = new byte[numBytes];
      data.get(ret, offset, numBytes);
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
