/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package com.pingcap.tikv.row;

import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.types.Converter;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.LogDesensitization;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

// A dummy implementation of Row interface
// Using non-memory compact format
public class ObjectRowImpl implements Row {
  private final Object[] values;

  private ObjectRowImpl(Object[] values) {
    this.values = values;
  }

  private ObjectRowImpl(int fieldCount) {
    values = new Object[fieldCount];
  }

  public static Row create(Object[] values) {
    return new ObjectRowImpl(values);
  }

  public static Row create(int fieldCount) {
    return new ObjectRowImpl(fieldCount);
  }

  @Override
  public void setNull(int pos) {
    values[pos] = null;
  }

  @Override
  public boolean isNull(int pos) {
    return values[pos] == null;
  }

  @Override
  public void setFloat(int pos, float v) {
    values[pos] = v;
  }

  @Override
  public float getFloat(int pos) {
    return (float) values[pos];
  }

  @Override
  public void setInteger(int pos, int v) {
    values[pos] = v;
  }

  @Override
  public int getInteger(int pos) {
    return (int) values[pos];
  }

  @Override
  public void setShort(int pos, short v) {
    values[pos] = v;
  }

  @Override
  public short getShort(int pos) {
    return (short) values[pos];
  }

  @Override
  public void setDouble(int pos, double v) {
    values[pos] = v;
  }

  @Override
  public double getDouble(int pos) {
    // Null should be handled by client code with isNull
    // below all get method behave the same
    return (double) values[pos];
  }

  @Override
  public void setLong(int pos, long v) {
    values[pos] = v;
  }

  @Override
  public long getLong(int pos) {
    // Null should be handled by client code with isNull
    // below all get method behave the same
    return (long) values[pos];
  }

  @Override
  public long getUnsignedLong(int pos) {
    return ((BigDecimal) values[pos]).longValue();
  }

  @Override
  public void setString(int pos, String v) {
    values[pos] = v;
  }

  @Override
  public String getString(int pos) {
    return Converter.convertToString(values[pos]);
  }

  @Override
  public void setTime(int pos, Time v) {
    values[pos] = v;
  }

  @Override
  public Date getTime(int pos) {
    return (Date) values[pos];
  }

  @Override
  public void setTimestamp(int pos, Timestamp v) {
    values[pos] = v;
  }

  @Override
  public Timestamp getTimestamp(int pos) {
    return (Timestamp) values[pos];
  }

  @Override
  public void setDate(int pos, Date v) {
    values[pos] = v;
  }

  @Override
  public Date getDate(int pos) {
    return (Date) values[pos];
  }

  @Override
  public void setBytes(int pos, byte[] v) {
    values[pos] = v;
  }

  @Override
  public byte[] getBytes(int pos) {
    return (byte[]) values[pos];
  }

  @Override
  public void set(int pos, DataType type, Object v) {
    // Ignore type for this implementation since no serialization happens
    values[pos] = v;
  }

  @Override
  public Object get(int pos, DataType type) {
    // Ignore type for this implementation since no serialization happens
    return values[pos];
  }

  @Override
  public int fieldCount() {
    return values.length;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("(");
    for (int i = 0; i < values.length; i++) {
      if (values[i] instanceof byte[]) {
        builder.append("[");
        builder.append(LogDesensitization.hide(KeyUtils.formatBytes(((byte[]) values[i]))));
        builder.append("]");
      } else if (values[i] instanceof BigDecimal) {
        builder.append(((BigDecimal) values[i]).toPlainString());
      } else {
        builder.append(values[i]);
      }
      if (i < values.length - 1) {
        builder.append(",");
      }
    }
    builder.append(")");
    return builder.toString();
  }
}
