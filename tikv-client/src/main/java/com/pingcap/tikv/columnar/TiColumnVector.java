/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tikv.columnar;

import com.pingcap.tikv.types.DataType;
import java.math.BigDecimal;

/**
 * An interface representing in-memory columnar data in Spark. This interface defines the main APIs
 * to access the data, as well as their batched versions. The batched versions are considered to be
 * faster and preferable whenever possible.
 *
 * <p>Most of the APIs take the rowId as a parameter. This is the batch local 0-based row id for
 * values in this ColumnVector.
 *
 * <p>Spark only calls specific `get` method according to the data type of this {@link
 * TiColumnVector}, e.g. if it's int type, Spark is guaranteed to only call {@link #getInt(int)} or
 * {@link #getInts(int, int)}.
 *
 * <p>ColumnVector is expected to be reused during the entire data loading process, to avoid
 * allocating memory again and again.
 *
 * <p>ColumnVector is meant to maximize CPU efficiency but not to minimize storage footprint.
 * Implementations should prefer computing efficiency over storage efficiency when design the
 * format. Since it is expected to reuse the ColumnVector instance while loading data, the storage
 * footprint is negligible.
 */
public abstract class TiColumnVector implements AutoCloseable {

  /** Returns the data type of this column vector. */
  public final DataType dataType() {
    return type;
  }

  /**
   * Cleans up memory for this column vector. The column vector is not usable after this.
   *
   * <p>This overwrites `AutoCloseable.close` to remove the `throws` clause, as column vector is
   * in-memory and we don't expect any exception to happen during closing.
   */
  @Override
  public abstract void close();

  /** Returns true if this column vector contains any null values. */
  public abstract boolean hasNull();

  /** Returns the number of nulls in this column vector. */
  public abstract int numNulls();

  /** Returns whether the value at rowId is NULL. */
  public abstract boolean isNullAt(int rowId);

  /**
   * Returns the boolean type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public abstract boolean getBoolean(int rowId);

  /**
   * Gets boolean type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public boolean[] getBooleans(int rowId, int count) {
    boolean[] res = new boolean[count];
    for (int i = 0; i < count; i++) {
      res[i] = getBoolean(rowId + i);
    }
    return res;
  }

  /**
   * Returns the byte type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public abstract byte getByte(int rowId);

  /**
   * Gets byte type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public byte[] getBytes(int rowId, int count) {
    byte[] res = new byte[count];
    for (int i = 0; i < count; i++) {
      res[i] = getByte(rowId + i);
    }
    return res;
  }

  /**
   * Returns the short type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public abstract short getShort(int rowId);

  /**
   * Gets short type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public short[] getShorts(int rowId, int count) {
    short[] res = new short[count];
    for (int i = 0; i < count; i++) {
      res[i] = getShort(rowId + i);
    }
    return res;
  }

  /**
   * Returns the int type value for rowId. The return value is undefined and can be anything, if the
   * slot for rowId is null.
   */
  public abstract int getInt(int rowId);

  /**
   * Gets int type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public int[] getInts(int rowId, int count) {
    int[] res = new int[count];
    for (int i = 0; i < count; i++) {
      res[i] = getInt(rowId + i);
    }
    return res;
  }

  /**
   * Returns the long type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public abstract long getLong(int rowId);

  /**
   * Gets long type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public long[] getLongs(int rowId, int count) {
    long[] res = new long[count];
    for (int i = 0; i < count; i++) {
      res[i] = getLong(rowId + i);
    }
    return res;
  }

  /**
   * Returns the float type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public abstract float getFloat(int rowId);

  /**
   * Gets float type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public float[] getFloats(int rowId, int count) {
    float[] res = new float[count];
    for (int i = 0; i < count; i++) {
      res[i] = getFloat(rowId + i);
    }
    return res;
  }

  /**
   * Returns the double type value for rowId. The return value is undefined and can be anything, if
   * the slot for rowId is null.
   */
  public abstract double getDouble(int rowId);

  /**
   * Gets double type values from [rowId, rowId + count). The return values for the null slots are
   * undefined and can be anything.
   */
  public double[] getDoubles(int rowId, int count) {
    double[] res = new double[count];
    for (int i = 0; i < count; i++) {
      res[i] = getDouble(rowId + i);
    }
    return res;
  }

  /**
   * Returns the decimal type value for rowId. If the slot for rowId is null, it should return null.
   */
  public abstract BigDecimal getDecimal(int rowId, int precision, int scale);

  /**
   * Returns the string type value for rowId. If the slot for rowId is null, it should return null.
   * Note that the returned UTF8String may point to the data of this column vector, please copy it
   * if you want to keep it after this column vector is freed.
   */
  public abstract String getUTF8String(int rowId);

  /**
   * Returns the binary type value for rowId. If the slot for rowId is null, it should return null.
   */
  public abstract byte[] getBinary(int rowId);

  /** @return child [[TiColumnVector]] at the given ordinal. */
  protected abstract TiColumnVector getChild(int ordinal);

  /** Data type for this column. */
  protected DataType type;

  /** Sets up the data type of this column vector. */
  protected TiColumnVector(DataType type) {
    this.type = type;
  }
}
