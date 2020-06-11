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

import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.DataType;
import java.math.BigDecimal;

/**
 * An implementation of {@link TiColumnVector}. It is a faked column vector; the underlying data is
 * in row format.
 */
public class TiRowColumnVector extends TiColumnVector {
  /** Represents the column index of original row */
  private final int colIdx;
  /** row-wise format data and data is already decoded */
  private Row[] rows;
  /** Sets up the data type of this column vector. */
  public TiRowColumnVector(DataType type, int colIdx, Row[] rows, int numOfRows) {
    super(type, numOfRows);
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
    return ((Number) rows[rowId].getDouble(colIdx)).floatValue();
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
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    Object val = rows[rowId].get(colIdx, null);
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    }

    if (val instanceof Long) {
      return BigDecimal.valueOf((long) val);
    }

    throw new UnsupportedOperationException(
        String.format(
            "failed to getDecimal and the value is %s:%s", val.getClass().getCanonicalName(), val));
  }

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  @Override
  public String getUTF8String(int rowId) {
    return rows[rowId].getString(colIdx);
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
    throw new UnsupportedOperationException(
        "row-wise column vector does not support this operation");
  }
}
