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

import com.pingcap.tikv.datatype.TypeMapping;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarArray;
import org.apache.spark.sql.vectorized.ColumnarMap;
import org.apache.spark.unsafe.types.UTF8String;

public class TiColumnVectorAdapter extends ColumnVector {
  private final TiColumnVector tiColumnVector;

  /** Sets up the data type of this column vector. */
  public TiColumnVectorAdapter(TiColumnVector tiColumnVector) {
    super(TypeMapping.toSparkType(tiColumnVector.dataType()));
    this.tiColumnVector = tiColumnVector;
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  public void close() {}

  /** Returns true if this column vector contains any null values. */
  public boolean hasNull() {
    return tiColumnVector.hasNull();
  }

  /** Returns the number of nulls in this column vector. */
  public int numNulls() {
    return tiColumnVector.numNulls();
  }

  /** Returns whether the value at rowId is NULL. */
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
   * Returns the array type value for rowId. If the slot for rowId is null, it should return null.
   *
   * <p>To support array type, implementations must construct an {@link ColumnarArray} and return it
   * in this method. {@link ColumnarArray} requires a {@link ColumnVector} that stores the data of
   * all the elements of all the arrays in this vector, and an offset and length which points to a
   * range in that {@link ColumnVector}, and the range represents the array for rowId.
   * Implementations are free to decide where to put the data vector and offsets and lengths. For
   * example, we can use the first child vector as the data vector, and store offsets and lengths in
   * 2 int arrays in this vector.
   */
  @Override
  public ColumnarArray getArray(int rowId) {
    throw new UnsupportedOperationException("TiColumnVectorAdapter is not supported this method");
  }

  /**
   * Returns the map type value for rowId. If the slot for rowId is null, it should return null.
   *
   * <p>In Spark, map type value is basically a key data array and a value data array. A key from
   * the key array with a index and a value from the value array with the same index contribute to
   * an entry of this map type value.
   *
   * <p>To support map type, implementations must construct a {@link ColumnarMap} and return it in
   * this method. {@link ColumnarMap} requires a {@link ColumnVector} that stores the data of all
   * the keys of all the maps in this vector, and another {@link ColumnVector} that stores the data
   * of all the values of all the maps in this vector, and a pair of offset and length which specify
   * the range of the key/value array that belongs to the map type value at rowId.
   */
  @Override
  public ColumnarMap getMap(int ordinal) {
    throw new UnsupportedOperationException("TiColumnVectorAdapter is not supported this method");
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  public Decimal getDecimal(int rowId, int precision, int scale) {
    return Decimal.apply(tiColumnVector.getDecimal(rowId, precision, scale));
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

  /** @return child [[ColumnVector]] at the given ordinal. */
  @Override
  public ColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException("TiColumnVectorAdapter is not supported this method");
  }
}
