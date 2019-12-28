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
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;

// https://dev.mysql.com/doc/refman/8.0/en/time.html

public class TimeType extends DataType {
  public static final TimeType TIME = new TimeType(MySQLType.TypeDuration);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeDuration};
  protected static final long NANOSECOND = 1;
  protected static final long MICROSECOND = 1000 * NANOSECOND;
  protected static final long MILLISECOND = 1000 * MICROSECOND;
  protected static final long SECOND = 1000 * MILLISECOND;
  protected static final long MINUTE = 60 * SECOND;
  protected static final long HOUR = 60 * MINUTE;

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

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == Codec.VARINT_FLAG) {
      return IntegerCodec.readVarLong(cdi);
    } else if (flag == Codec.DURATION_FLAG) {
      return IntegerCodec.readLong(cdi);
    }
    throw new TypeException("Invalid TimeType flag: " + flag);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
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
    // in tidb, duration will be firstly flatten into int64 and then encoded.
    if (value instanceof Long) {
      IntegerCodec.writeLong(cdo, (long) value);
    } else {
      long val = Converter.convertStrToDuration(Converter.convertToString(value));
      IntegerCodec.writeLong(cdo, val);
    }
  }

  @Override
  public String getName() {
    return "TIME";
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlDuration;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return Converter.convertStrToDuration(value);
  }
}
