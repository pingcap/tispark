package com.pingcap.tikv.columnar;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.MemoryUtil;
import com.pingcap.tikv.util.TypeMapping;
import java.nio.ByteBuffer;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class TiChunkColumn extends TiColumnVector {
  protected DataType dataType;
  private int numOfRows;
  private int numOfNulls;
  private byte[] nullBitMaps;
  private ByteBuffer data;
  private long dataAddr;

  public TiChunkColumn(
      DataType dataType, int numOfRows, int numOfNulls, byte[] nullBitMaps, ByteBuffer data) {
    super(TypeMapping.toSparkType(dataType));
    this.dataType = dataType;
    this.numOfRows = numOfRows;
    this.numOfNulls = numOfNulls;
    this.nullBitMaps = nullBitMaps;
    this.data = data;
    this.dataAddr = MemoryUtil.getAddress(data);
  }

  public final String typeName() {
    return dataType().typeName();
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

  public long getLong(int rowId) {
    return MemoryUtil.getLong(dataAddr);
  }

  public float getFloat(int rowId) {
    throw new UnsupportedOperationException();
  }

  public double getDouble(int rowId) {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the array type value for rowId. If the slot for rowId is null, it should return null.
   *
   * <p>To support array type, implementations must construct an {@link TiColumnarArray} and return
   * it in this method. {@link TiColumnarArray} requires a {@link TiColumnVector} that stores the
   * data of all the elements of all the arrays in this vector, and an offset and length which
   * points to a range in that {@link TiColumnVector}, and the range represents the array for rowId.
   * Implementations are free to decide where to put the data vector and offsets and lengths. For
   * example, we can use the first child vector as the data vector, and store offsets and lengths in
   * 2 int arrays in this vector.
   */
  @Override
  public TiColumnarArray getArray(int rowId) {
    return null;
  }

  /**
   * Returns the map type value for rowId. If the slot for rowId is null, it should return null.
   *
   * <p>In Spark, map type value is basically a key data array and a value data array. A key from
   * the key array with a index and a value from the value array with the same index contribute to
   * an entry of this map type value.
   *
   * <p>To support map type, implementations must construct a {@link TiColumnarMap} and return it in
   * this method. {@link TiColumnarMap} requires a {@link TiColumnVector} that stores the data of
   * all the keys of all the maps in this vector, and another {@link TiColumnVector} that stores the
   * data of all the values of all the maps in this vector, and a pair of offset and length which
   * specify the range of the key/value array that belongs to the map type value at rowId.
   */
  @Override
  public TiColumnarMap getMap(int ordinal) {
    return null;
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return null;
  }

  public Decimal getDecimal(int rowId) {
    throw new UnsupportedOperationException();
  }

  public UTF8String getUTF8String(int rowId) {
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

  /**
   * After done with insertion, you should call this method to make the inserted data readable.
   * Mainly used to avoid frequently reallocating memory.
   */
}
