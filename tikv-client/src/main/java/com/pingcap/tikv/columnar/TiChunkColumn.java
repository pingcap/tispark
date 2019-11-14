package com.pingcap.tikv.columnar;

import com.pingcap.tikv.types.DataType;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class TiChunkColumn extends TiColumnVector {
  protected DataType dataType;
  private int numOfRows;
  private int numOfNulls;
  private byte[] nullBitMaps;
  private ByteBuffer data;

  public TiChunkColumn(
      DataType dataType, int numOfRows, int numOfNulls, byte[] nullBitMaps, ByteBuffer data) {
    super(dataType);
    this.dataType = dataType;
    this.numOfRows = numOfRows;
    this.numOfNulls = numOfNulls;
    this.nullBitMaps = nullBitMaps;
    this.data = data;
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

  public int numOfRows() {
    return numOfRows;
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

  public long getLong(int rowId) {
    // TODO handle date/datetime/druation/timestamp later;
    return data.getLong();
  }

  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    return null;
  }

  public BigDecimal getDecimal(int rowId) {
    throw new UnsupportedOperationException();
  }

  public String getUTF8String(int rowId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    return new byte[0];
  }

  /** @return child [[TiColumnVector]] at the given ordinal. */
  @Override
  protected TiColumnVector getChild(int ordinal) {
    return null;
  }
}
