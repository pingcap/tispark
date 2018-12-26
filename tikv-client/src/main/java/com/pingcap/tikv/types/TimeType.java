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
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import java.sql.Time;

// https://dev.mysql.com/doc/refman/8.0/en/time.html
public class TimeType extends DataType {
  private Time

  protected TimeType(InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.UINT_FLAG) throw new TypeException("Invalid EnumType(IntegerType) flag: " + flag);
    long pockedInt = IntegerCodec.readULong(cdi);
    // case mysql.TypeDuration: //duration should read fsp from column meta data
    //		dur := types.Duration{Duration: time.Duration(datum.GetInt64()), Fsp: ft.Decimal}
    //		datum.SetValue(dur)
    //		return datum, nil
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    // b = append(b, uintFlag)
    //			t := vals[i].GetMysqlTime()
    //			// Encoding timestamp need to consider timezone.
    //			// If it's not in UTC, transform to UTC first.
    //			if t.Type == mysql.TypeTimestamp && sc.TimeZone != time.UTC {
    //				err = t.ConvertTimeZone(sc.TimeZone, time.UTC)
    //				if err != nil {
    //					return nil, errors.Trace(err)
    //				}
    //			}
    //			var v uint64
    //			v, err = t.ToPackedUint()
    //			if err != nil {
    //				return nil, errors.Trace(err)
    //			}
    //			b = EncodeUint(b, v)
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {

  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {

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
