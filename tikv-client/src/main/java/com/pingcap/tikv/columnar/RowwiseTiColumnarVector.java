package com.pingcap.tikv.columnar;

import com.pingcap.tikv.row.Row;
import java.math.BigDecimal;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class RowwiseTiColumnarVector extends TiColumnVector {

  private Row[] rows;
  private int colIdx;
  /** Sets up the data type of this column vector. */
  public RowwiseTiColumnarVector(DataType type, int colIdx, Row[] rows) {
    super(type);
    this.rows = rows;
    this.colIdx = colIdx;
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  @Override
  public void close() {
    this.rows = null;
  }

  /** Returns true if this column vector contains any null values. */
  @Override
  public boolean hasNull() {
    throw new UnsupportedOperationException(
        "row-wise column vector does not support this operation");
  }

  /** Returns the number of nulls in this column vector. */
  @Override
  public int numNulls() {
    throw new UnsupportedOperationException(
        "row-wise column vector does not support this operation");
  }

  /** Returns whether the value at rowId is NULL. */
  @Override
  public boolean isNullAt(int rowId) {
    return rows[rowId].get(colIdx, null) == null;
  }

  /**
   * Returns the boolean type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public boolean getBoolean(int rowId) {
    return rows[rowId].getLong(colIdx) == 1;
  }

  /**
   * Returns the byte type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public byte getByte(int rowId) {
    return (byte) rows[rowId].getLong(colIdx);
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public short getShort(int rowId) {
    return (short) rows[rowId].getLong(colIdx);
  }

  /**
   * Returns the int type value for rowId. The return value is undefined and can be anything, if the
   * slot for rowId is null.
   */
  @Override
  public int getInt(int rowId) {
    return (int) rows[rowId].getLong(colIdx);
  }

  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public long getLong(int rowId) {
    return rows[rowId].getLong(colIdx);
  }

  /**
   * Returns the float type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public float getFloat(int rowId) {
    return 0;
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public double getDouble(int rowId) {
    return rows[rowId].getDouble(colIdx);
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
    throw new UnsupportedOperationException(
        "row-wise column vector does not support this operation");
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
    throw new UnsupportedOperationException(
        "row-wise column vector does not support this operation");
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return Decimal.apply((BigDecimal) rows[rowId].get(colIdx, null));
  }

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  @Override
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromString(rows[rowId].getString(colIdx));
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    return rows[rowId].getBytes(colIdx);
  }

  /** @return child [[TiColumnVector]] at the given ordinal. */
  @Override
  protected TiColumnVector getChild(int ordinal) {
    return null;
  }
}
