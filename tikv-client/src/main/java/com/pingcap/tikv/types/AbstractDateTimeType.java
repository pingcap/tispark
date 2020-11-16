/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tikv.types;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.ExtendedDateTime;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.DateCodec;
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo.InternalTypeHolder;
import java.sql.Timestamp;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;

public abstract class AbstractDateTimeType extends DataType {

  AbstractDateTimeType(InternalTypeHolder holder) {
    super(holder);
  }

  AbstractDateTimeType(MySQLType tp) {
    super(tp);
  }

  public abstract DateTimeZone getTimezone();
  /**
   * Decode DateTime from packed long value In TiDB / MySQL, timestamp type is converted to UTC and
   * stored
   */
  long decodeDateTime(int flag, CodecDataInput cdi) {
    ExtendedDateTime extendedDateTime;
    if (flag == Codec.UVARINT_FLAG) {
      extendedDateTime = DateTimeCodec.readFromUVarInt(cdi, getTimezone());
    } else if (flag == Codec.UINT_FLAG) {
      extendedDateTime = DateTimeCodec.readFromUInt(cdi, getTimezone());
    } else {
      throw new InvalidCodecFormatException(
          "Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }

    // Even though null is filtered out but data like 0000-00-00 exists
    // according to MySQL JDBC behavior, it can chose the **ROUND** behavior converted to the
    // nearest
    // value which is 0001-01-01.
    if (extendedDateTime == null) {
      extendedDateTime = DateTimeCodec.createExtendedDateTime(getTimezone(), 1, 1, 1, 0, 0, 0, 0);
    }
    Timestamp ts = extendedDateTime.toTimeStamp();
    return ts.getTime() / 1000 * 1000000 + ts.getNanos() / 1000;
  }

  Timestamp decodeDateTimeForBatchWrite(int flag, CodecDataInput cdi) {
    ExtendedDateTime extendedDateTime;
    if (flag == Codec.UVARINT_FLAG) {
      extendedDateTime = DateTimeCodec.readFromUVarInt(cdi, getTimezone());
    } else if (flag == Codec.UINT_FLAG) {
      extendedDateTime = DateTimeCodec.readFromUInt(cdi, getTimezone());
    } else {
      throw new InvalidCodecFormatException(
          "Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    if (extendedDateTime == null) {
      return DateTimeCodec.createExtendedDateTime(getTimezone(), 1, 1, 1, 0, 0, 0, 0).toTimeStamp();
    }
    return extendedDateTime.toTimeStamp();
  }

  /** Decode Date from packed long value */
  LocalDate decodeDate(int flag, CodecDataInput cdi) {
    LocalDate date;
    if (flag == Codec.UVARINT_FLAG) {
      date = DateCodec.readFromUVarInt(cdi);
    } else if (flag == Codec.UINT_FLAG) {
      date = DateCodec.readFromUInt(cdi);
    } else {
      throw new InvalidCodecFormatException(
          "Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    return date;
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    ExtendedDateTime edt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeFully(cdo, edt, getTimezone());
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    encodeKey(cdo, value);
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    ExtendedDateTime edt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeProto(cdo, edt, getTimezone());
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlTime;
  }

  java.sql.Timestamp convertToMysqlDateTime(Object value) throws ConvertNotSupportException {
    java.sql.Timestamp result;
    if (value instanceof String) {
      result = java.sql.Timestamp.valueOf((String) value);
    } else if (value instanceof java.sql.Date) {
      result = new java.sql.Timestamp(((java.sql.Date) value).getTime());
    } else if (value instanceof java.sql.Timestamp) {
      result = (java.sql.Timestamp) value;
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }
    return result;
  }
}
