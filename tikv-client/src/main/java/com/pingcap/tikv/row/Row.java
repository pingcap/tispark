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

import com.pingcap.tikv.types.DataType;
import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * Even in case of mem-buffer-based row we can ignore field types when en/decoding if we put some
 * padding bits for fixed length and use fixed length index for var-length
 */
public interface Row extends Serializable {
  void setNull(int pos);

  boolean isNull(int pos);

  void setFloat(int pos, float v);

  float getFloat(int pos);

  void setDouble(int pos, double v);

  double getDouble(int pos);

  void setInteger(int pos, int v);

  int getInteger(int pos);

  void setShort(int pos, short v);

  short getShort(int pos);

  void setLong(int pos, long v);

  long getLong(int pos);

  long getUnsignedLong(int pos);

  void setString(int pos, String v);

  String getString(int pos);

  void setTime(int pos, Time v);

  Date getTime(int pos);

  void setTimestamp(int pos, Timestamp v);

  Timestamp getTimestamp(int pos);

  void setDate(int pos, Date v);

  Date getDate(int pos);

  void setBytes(int pos, byte[] v);

  byte[] getBytes(int pos);

  void set(int pos, DataType type, Object v);

  Object get(int pos, DataType type);

  int fieldCount();
}
