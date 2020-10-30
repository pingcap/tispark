/*
 * Copyright 2019 PingCAP, Inc.
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

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.MyDecimal;
import com.pingcap.tikv.types.AbstractDateTimeType;
import com.pingcap.tikv.types.BitType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.EnumType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.JsonType;
import com.pingcap.tikv.types.TimeType;
import com.pingcap.tikv.types.TimestampType;
import com.pingcap.tikv.util.JsonUtils;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import org.joda.time.LocalDate;

/** An implementation of {@link TiColumnVector}. All data is stored in TiDB chunk format. */
public class TiChunkColumnVector extends TiColumnVector {
  /** Represents the length of each different data type */
  private final int fixLength;
  /** Represents how many nulls in this column vector */
  private final int numOfNulls;
  /** Can be used to determine data at rowId is null or not */
  private final byte[] nullBitMaps;
  /** Can be used to read non-fixed length data type such as string */
  private final long[] offsets;

  private final ByteBuffer data;

  public TiChunkColumnVector(
      DataType dataType,
      int fixLength,
      int numOfRows,
      int numOfNulls,
      byte[] nullBitMaps,
      long[] offsets,
      ByteBuffer data) {
    super(dataType, numOfRows);
    this.fixLength = fixLength;
    this.numOfNulls = numOfNulls;
    this.nullBitMaps = nullBitMaps;
    this.data = data;
    this.offsets = offsets;
  }

  public final String typeName() {
    return dataType().getType().name();
  }

  // TODO: once we switch off_heap mode, we need control memory access pattern.
  public void free() {}

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
    return numOfNulls > 0;
  }

  /** Returns the number of nulls in this column vector. */
  @Override
  public int numNulls() {
    return numOfNulls;
  }

  public boolean isNullAt(int rowId) {
    int nullByte = this.nullBitMaps[rowId / 8] & 0XFF;
    return (nullByte & (1 << (rowId & 7))) == 0;
  }

  /**
   * Returns the boolean type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public boolean getBoolean(int rowId) {
    return false;
  }

  public byte getByte(int rowId) {
    return data.get();
  }

  public short getShort(int rowId) {
    return data.getShort();
  }

  public int getInt(int rowId) {
    return (int) getLong(rowId);
  }

  private boolean isDataTimeOrTimestamp() {
    return type instanceof DateTimeType || type instanceof TimestampType;
  }

  private long getTime(int rowId) {
    int startPos = rowId * fixLength;
    TiCoreTime coreTime = new TiCoreTime(data.getLong(startPos));

    int year = coreTime.getYear();
    int month = coreTime.getMonth();
    int day = coreTime.getDay();
    int hour = coreTime.getHour();
    int minute = coreTime.getMinute();
    int second = coreTime.getSecond();
    long microsecond = coreTime.getMicroSecond();
    // This behavior can be modified using the zeroDateTimeBehavior configuration property.
    // The allowable values are:
    //    * exception (the default), which throws an SQLException with an SQLState of S1009.
    //    * convertToNull, which returns NULL instead of the date.
    //    * round, which rounds the date to the nearest closest value which is 0001-01-01.
    if (year == 0 && month == 0 && day == 0 && hour == 0 && minute == 0 && microsecond == 0) {
      year = 1;
      month = 1;
      day = 1;
    }
    if (this.type instanceof DateType) {
      LocalDate date = new LocalDate(year, month, day);
      return ((DateType) this.type).getDays(date);
    } else if (type instanceof DateTimeType || type instanceof TimestampType) {
      // only return microsecond from epoch.
      Timestamp ts =
          new Timestamp(
              year - 1900, month - 1, day, hour, minute, second, (int) microsecond * 1000);
      return ts.getTime() / 1000 * 1000000 + ts.getNanos() / 1000;
    } else {
      throw new UnsupportedOperationException("data, datetime, timestamp are already handled.");
    }
  }

  private long getLongFromBinary(int rowId) {
    byte[] bytes = getBinary(rowId);
    if (bytes.length == 0) return 0;
    long result = 0;
    for (byte b : bytes) {
      result = (result << 8) | b;
    }
    return result;
  }

  public long getLong(int rowId) {
    if (type instanceof IntegerType) {
      if (type instanceof BitType) {
        return getLongFromBinary(rowId);
      }
      return data.getLong(rowId * fixLength);
    } else if (type instanceof AbstractDateTimeType) {
      return getTime(rowId);
    } else if (type instanceof TimeType) {
      return data.getLong(rowId * fixLength);
    }

    throw new UnsupportedOperationException("only IntegerType and Time related are supported.");
  }

  public float getFloat(int rowId) {
    return data.getFloat(rowId * fixLength);
  }

  public double getDouble(int rowId) {
    return data.getDouble(rowId * fixLength);
  }

  private MyDecimal getMyDecimal(int rowId) {
    int startPos = rowId * fixLength;
    int digitsInt = data.get(startPos);
    int digitsFrac = data.get(startPos + 1);
    int resultFrac = data.get(startPos + 2);
    boolean negative = data.get(startPos + 3) == 1;
    int[] wordBuf = new int[9];
    for (int i = 0; i < 9; i++) {
      wordBuf[i] = data.getInt(startPos + 4 + i * 4);
    }

    return new MyDecimal(digitsInt, digitsFrac, negative, wordBuf);
  }
  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  /** digitsInt int8 1 digitsFrac int8 1 resultFrac int8 1 negative bool 1 wordBuf int32[9] 36 */
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    // this is to handle unsigned long to avoid overflow.
    if (type instanceof IntegerType) {
      return new BigDecimal(UnsignedLong.fromLongBits(this.getLong(rowId)).bigIntegerValue());
    }
    // TODO figure out how to use precision and scale
    MyDecimal decimal = getMyDecimal(rowId);
    return decimal.toBigDecimal();
  }

  private String getEnumString(int rowId) {
    int start = (int) this.offsets[rowId];
    long end = this.offsets[rowId + 1];
    return new String(getRawBinary(start + 8, end));
  }

  private String getJsonString(int rowId) {
    long start = this.offsets[rowId];
    long end = this.offsets[rowId + 1];
    return JsonUtils.parseJson(new CodecDataInput(getRawBinary(start, end))).toString();
  }

  public String getUTF8String(int rowId) {
    if (type instanceof EnumType) {
      return getEnumString(rowId);
    }

    if (type instanceof JsonType) {
      return getJsonString(rowId);
    }

    return new String(getBinary(rowId));
  }

  private byte[] getRawBinary(long start, long end) {
    byte[] buffer = new byte[(int) (end - start)];
    for (int i = 0; i < (end - start); i++) {
      buffer[i] = data.get((int) (start + i));
    }
    return buffer;
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    int start = (int) this.offsets[rowId];
    long end = this.offsets[rowId + 1];
    return getRawBinary(start, end);
  }

  /** @return child [[TiColumnVector]] at the given ordinal. */
  @Override
  protected TiColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("TiChunkColumnVector does not support this operation");
  }
}
