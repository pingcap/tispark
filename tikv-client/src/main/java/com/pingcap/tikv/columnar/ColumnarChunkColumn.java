package com.pingcap.tikv.columnar;

import com.pingcap.tikv.codec.MyDecimal;
import com.pingcap.tikv.types.AbstractDateTimeType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;
import com.pingcap.tikv.types.DateType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.MySQLType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.Date;

public class ColumnarChunkColumn extends TiColumnVector {
  protected DataType dataType;
  private int numOfNulls;
  private byte[] nullBitMaps;
  private long[] offsets;
  private ByteBuffer data;

  public ColumnarChunkColumn(
      DataType dataType, int numOfRows, int numOfNulls, byte[] nullBitMaps, long[] offsets, ByteBuffer data) {
    super(dataType, numOfRows);
    this.dataType = dataType;
    this.numOfNulls = numOfNulls;
    this.nullBitMaps = nullBitMaps;
    this.data = data;
    this.offsets = offsets;
  }

  public final String typeName() {
    return dataType().getType().name();
  }

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
    int nullByte = this.nullBitMaps[rowId / 8] & 0xff;
    return (nullByte & (1 << (rowId) & 7)) == 0;
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
    throw new UnsupportedOperationException();
  }

  public short getShort(int rowId) {
    throw new UnsupportedOperationException();
  }

  public int getInt(int rowId) {
    throw new UnsupportedOperationException();
  }

  // Time in TiDB has the following memory layout:
  // name         type    num of bytes
  // hour         uint32  4
  // microsecond  uint32  4
  // year         uint16  2
  // month        uint16  2
  // day          uint8   1
  // minute       uint8   1
  // second       uint8   1
  // type         uint8   1
  // Fsp          int8    1
  private long getTime(int rowId) {
    long hour = data.getInt();
    long microsecond = data.getInt();
    int year = data.getShort();
    int month = data.getShort();
    int day = data.get();
    int minute = data.get();
    int second = data.get();
    int type = data.get();
    int fsp = data.get();
    // data case
    if(type == MySQLType.TypeDate.getTypeCode()) {
      // only return day from epoch
      return new Date(year, month, day).getTime()/(24 * 60 * 60 * 1000);
    } else if(type == MySQLType.TypeDatetime.getTypeCode()) {
      // TODO: only return microseconds from epoch
      // timezone convert is needed here
      return new Timestamp(year, month, day,
          (int)hour, minute, second, (int)microsecond).getTime() * 1000;
    } else if(type == MySQLType.TypeTimestamp.getTypeCode()) {
      // only return millisecond from epoch.
      return new Timestamp(year, month, day,
          (int)hour, minute, second, (int)microsecond).getTime() * 1000;
    } else {
      throw new UnsupportedOperationException("data, datetime, timestamp are already handled.");
    }
  }

  public long getLong(int rowId) {
    // TODO handle date/datetime/duration/timestamp later;
    if(type instanceof IntegerType) {
      return data.getLong();
    } else if(type instanceof AbstractDateTimeType) {
      return getTime(rowId);
    }

    throw new UnsupportedOperationException("only IntegerType and TimeType are supported.");
  }

  public float getFloat(int rowId) {
    return (float) data.getDouble();
  }

  public double getDouble(int rowId) {
    return data.getDouble();
  }

  private MyDecimal getMyDecimal(int rowId) {
    int digitsInt = data.getInt();
    int digitsFrac  = data.getInt();
    int resultFrac  = data.getInt();
    boolean negative = data.get() == 1;
    int[] wordBuf = new int[9];
    for(int i = 0; i < 9; i++) {
      wordBuf[i] = data.getInt();
    }

    return new MyDecimal(digitsInt, digitsFrac, negative, wordBuf);
  }
  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  /**
   * digitsInt int8  1
   * digitsFrac int8 1
   * resultFrac int8 1
   * negative bool    1
   * wordBuf int32[9] 36
   */
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    //TODO figure out how to use precision and scale
    MyDecimal decimal = getMyDecimal(rowId);
    BigDecimal bigDecimal = new BigDecimal(decimal.toString());
    return bigDecimal;
  }

  public BigDecimal getDecimal(int rowId) {
    MyDecimal decimal = getMyDecimal(rowId);
    return new BigDecimal(decimal.toString());
  }

  public String getUTF8String(int rowId) {
    return new String(getBinary(rowId));
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    int start = (int) this.offsets[rowId];
    long end = this.offsets[rowId+1];
    byte[] buffer = new byte[(int) (end - start)];
    for (int i = start; i < end ; i++) {
      buffer[i] = data.get();
    }
    return buffer;
  }

  /** @return child [[TiColumnVector]] at the given ordinal. */
  @Override
  protected TiColumnVector getChild(int ordinal) {
    return null;
  }
}
