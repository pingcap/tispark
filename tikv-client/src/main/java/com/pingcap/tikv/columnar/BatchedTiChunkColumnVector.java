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

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/** An implementation of {@link TiColumnVector}. All data is stored in TiDB chunk format. */
public class BatchedTiChunkColumnVector extends TiColumnVector {
  private final List<TiChunkColumnVector> childColumns;
  private final int numOfNulls;
  private final int[] rightEndpoints;

  public BatchedTiChunkColumnVector(List<TiChunkColumnVector> child, int numOfRows) {
    super(child.get(0).dataType(), numOfRows);
    this.childColumns = child;
    this.numOfNulls =
        child
            .stream()
            .reduce(
                0,
                (partialAgeResult, columnVector) -> partialAgeResult + columnVector.numNulls(),
                Integer::sum);
    int right = 0;
    this.rightEndpoints = new int[child.size() + 1];
    this.rightEndpoints[0] = 0;
    for (int i = 1; i < rightEndpoints.length; i++) {
      right += child.get(i - 1).numOfRows();
      this.rightEndpoints[i] = right;
    }
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

  private int[] getColumnVectorIdxAndRowId(int rowId) {
    int offset = Arrays.binarySearch(this.rightEndpoints, rowId);
    int idx;
    if (offset >= 0) {
      idx = offset;
    } else {
      idx = -(offset + 2);
    }
    if (idx >= childColumns.size() || idx < 0) {
      throw new UnsupportedOperationException("Something goes wrong, it should never happen");
    }
    return new int[] {idx, rowId - rightEndpoints[idx]};
  }

  /** Returns whether the value at rowId is NULL. */
  @Override
  public boolean isNullAt(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).isNullAt(pair[1]);
  }

  /**
   * Returns the boolean type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public boolean getBoolean(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getBoolean(pair[1]);
  }

  /**
   * Returns the byte type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public byte getByte(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getByte(pair[1]);
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public short getShort(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getShort(pair[1]);
  }

  /**
   * Returns the int type value for rowId. The return value is undefined and can be anything, if the
   * slot for rowId is null.
   */
  @Override
  public int getInt(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getInt(pair[1]);
  }

  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public long getLong(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getLong(pair[1]);
  }

  /**
   * Returns the float type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public float getFloat(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getFloat(pair[1]);
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  @Override
  public double getDouble(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getDouble(pair[1]);
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public BigDecimal getDecimal(int rowId, int precision, int scale) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getDecimal(pair[1], precision, scale);
  }

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  @Override
  public String getUTF8String(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getUTF8String(pair[1]);
  }

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  @Override
  public byte[] getBinary(int rowId) {
    int[] pair = getColumnVectorIdxAndRowId(rowId);
    return childColumns.get(pair[0]).getBinary(pair[1]);
  }

  /** @return child [[TiColumnVector]] at the given ordinal. */
  @Override
  protected TiColumnVector getChild(int ordinal) {
    throw new UnsupportedOperationException(
        "TiChunkBatchColumnVector does not support this operation");
  }
}
