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

package com.pingcap.tikv.types;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;

// https://dev.mysql.com/doc/refman/8.0/en/time.html

public class TimeType extends DataType {
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeDuration};
  private final long nanosecond = 1;
  private final long microsecond = 1000 * nanosecond;
  private final long millisecond = 1000 * microsecond;
  private final long second = 1000 * millisecond;
  private final long minute = 60 * second;
  private final long hour = 60 * minute;
  private boolean splited;
  private int sign;
  private int hours;
  private int minutes;
  private int seconds;
  private int frac;

  @SuppressWarnings("unused")
  protected TimeType(InternalTypeHolder holder) {
    super(holder);
  }

  @SuppressWarnings("unused")
  protected TimeType(MySQLType type, int flag, int len, int decimal) {
    super(type, flag, len, decimal, "", Collation.DEF_COLLATION_CODE);
  }

  @SuppressWarnings("unused")
  protected TimeType(MySQLType tp) {
    super(tp);
  }

  private void splitDuration(long nanos) {
    int sign = 1;
    if (nanos < 0) {
      nanos = -nanos;
      sign = -1;
    }
    hours = (int) (nanos / hour);
    nanos -= hours * hour;
    minutes = (int) (nanos / minute);
    nanos -= minutes * minute;
    seconds = (int) (nanos / second);
    nanos -= seconds * second;
    frac = (int) (nanos / microsecond);
    splited = true;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (sign < 0) {
      sb.append('-');
    }
    sb.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    if (decimal > 0) {
      sb.append('.');
      sb.append(String.format("%06d", frac).substring(0, decimal));
    }
    return sb.toString();
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.VARINT_FLAG) throw new TypeException("Invalid TimeType flag: " + flag);
    splitDuration(IntegerCodec.readVarLong(cdi));
    return toString();
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    IntegerCodec.writeDuration(cdo, Converter.convertToLong(value));
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    // per tidb's implementation, comparable is not needed.
    encodeKey(cdo, value);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    encodeKey(cdo, value);
  }

  @Override
  public ExprType getProtoExprType() {
    return null;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return null;
  }
}
