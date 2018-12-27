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
  private final long NANOSECOND = 1;
  private final long MICROSECOND = 1000 * NANOSECOND;
  private final long MILLISECOND = 1000 * MICROSECOND;
  private final long SECOND = 1000 * MILLISECOND;
  private final long MINUTE = 60 * SECOND;
  private final long HOUR = 60 * MINUTE;
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
    sign = 1;
    if (nanos < 0) {
      nanos = -nanos;
      sign = -1;
    }
    hours = (int) (nanos / HOUR);
    nanos -= hours * HOUR;
    minutes = (int) (nanos / MINUTE);
    nanos -= minutes * MINUTE;
    seconds = (int) (nanos / SECOND);
    nanos -= seconds * SECOND;
    frac = (int) (nanos / MICROSECOND);
  }

  public String convertToStr() {
    StringBuilder sb = new StringBuilder();
    if (sign < 0) {
      sb.append('-');
    }
    sb.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    if (decimal > 0) {
      sb.append('.');
      sb.append(String.format("%06d", frac), 0, decimal);
    }
    return sb.toString();
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.VARINT_FLAG) throw new TypeException("Invalid TimeType flag: " + flag);
    splitDuration(IntegerCodec.readVarLong(cdi));
    return convertToStr();
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    IntegerCodec.writeDuration(cdo, parseTimeInStr(Converter.convertToString(value)));
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
    return ExprType.MysqlDuration;
  }

  private long parseTimeInStr(String value) {
    String[] splitBySemiColon = value.split(":");
    if (splitBySemiColon.length < 3)
      throw new IllegalArgumentException(
          String.format("%s is not a valid time type in mysql", value));
    int sign, hour, minute, second, frac;
    sign = 1;
    hour = Integer.parseInt(splitBySemiColon[0]);
    if (hour < 0) {
      sign = -1;
      hour -= hour;
    }
    minute = Integer.parseInt(splitBySemiColon[1]);
    if (splitBySemiColon[2].contains(".")) {
      String[] splitByDot = splitBySemiColon[2].split(".");
      second = Integer.parseInt(splitByDot[0]);
      frac = Integer.parseInt(splitByDot[1]);
    } else {
      second = Integer.parseInt(splitBySemiColon[2]);
      frac = 0;
    }
    return ((long) hour * HOUR
            + (long) minute * MINUTE
            + (long) second * SECOND
            + (long) frac * MICROSECOND)
        * sign;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return parseTimeInStr(value);
  }
}
