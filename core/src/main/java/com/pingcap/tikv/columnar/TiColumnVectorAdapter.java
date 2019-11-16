package com.pingcap.tikv.columnar;

import com.pingcap.tikv.datatype.TypeMapping;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

public class TiColumnVectorAdapter {
  private DataType dataType;
  private TiColumnVector tiColumnVector;
  /**
   * Sets up the data type of this column vector.
   */
  public TiColumnVectorAdapter(TiColumnVector tiColumnVector) {
    dataType = TypeMapping.toSparkType(tiColumnVector.dataType());
    this.tiColumnVector = tiColumnVector;
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  public void close() {

  }

  /**
   * Returns true if this column vector contains any null values.
   */
  public boolean hasNull() {
    return tiColumnVector.hasNull();
  }

  /**
   * Returns the number of nulls in this column vector.
   */
  public int numNulls() {
    return tiColumnVector.numNulls();
  }

  /**
   * Returns whether the value at rowId is NULL.
   */
  public boolean isNullAt(int rowId) {
    return tiColumnVector.isNullAt(rowId);
  }

  /**
   * Returns the boolean type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public boolean getBoolean(int rowId) {
    return tiColumnVector.getBoolean(rowId);
  }

  /**
   * Returns the byte type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public byte getByte(int rowId) {
    return tiColumnVector.getByte(rowId);
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public short getShort(int rowId) {
    return tiColumnVector.getShort(rowId);
  }

  /**
   * Returns the int type value for rowId. The return value is undefined and can be anything, if the
   * slot for rowId is null.
   */
  public int getInt(int rowId) {
    return tiColumnVector.getInt(rowId);
  }

  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public long getLong(int rowId) {
    return tiColumnVector.getLong(rowId);
  }

  /**
   * Returns the float type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public float getFloat(int rowId) {
    return tiColumnVector.getFloat(rowId);
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public double getDouble(int rowId) {
    return tiColumnVector.getDouble(rowId);
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return
   * null.
   */
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return  Decimal.apply(tiColumnVector.getDecimal(rowId, precision, scale));
  }

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  public UTF8String getUTF8String(int rowId) {
    return UTF8String.fromString(tiColumnVector.getUTF8String(rowId));
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  public byte[] getBinary(int rowId) {
    return tiColumnVector.getBinary(rowId);
  }

  public int numOfRows() {
    return tiColumnVector.numOfRows();
  }
}
