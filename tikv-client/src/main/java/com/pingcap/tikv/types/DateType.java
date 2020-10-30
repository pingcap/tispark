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

import com.pingcap.tikv.codec.Codec.DateCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Date;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.LocalDate;

public class DateType extends AbstractDateTimeType {
  private static final LocalDate EPOCH = new LocalDate(0);
  public static final DateType DATE = new DateType(MySQLType.TypeDate);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeDate};

  private DateType(MySQLType tp) {
    super(tp);
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public DateTimeZone getTimezone() {
    return Converter.getLocalTimezone();
  }

  @Override
  public Date getOriginDefaultValueNonNull(String value, long version) {
    return Converter.convertToDate(value);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToMysqlDate(value);
  }

  private java.sql.Date convertToMysqlDate(Object value) throws ConvertNotSupportException {
    java.sql.Date result;
    if (value instanceof Long) {
      result = new java.sql.Date((Long) value);
    } else if (value instanceof String) {
      result = java.sql.Date.valueOf((String) value);
    } else if (value instanceof java.sql.Date) {
      result = (java.sql.Date) value;
    } else if (value instanceof java.sql.Timestamp) {
      result = new java.sql.Date(((java.sql.Timestamp) value).getTime());
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    Date dt = Converter.convertToDate(value);
    DateCodec.writeDateFully(cdo, dt, getTimezone());
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    Date dt = Converter.convertToDate(value);
    DateCodec.writeDateProto(cdo, dt, getTimezone());
  }

  @Override
  public String getName() {
    return "DATE";
  }

  public int getDays(LocalDate d) {
    // count how many days from EPOCH
    int days = Days.daysBetween(EPOCH, d).getDays();
    // if the timezone has negative offset, minus one day.
    if (getTimezone().getOffset(0) < 0) {
      days -= 1;
    }
    return days;
  }

  /** {@inheritDoc} */
  @Override
  protected Long decodeNotNull(int flag, CodecDataInput cdi) {
    LocalDate date = decodeDate(flag, cdi);

    if (date == null) {
      return null;
    }

    return (long) getDays(date);
  }

  @Override
  protected Date decodeNotNullForBatchWrite(int flag, CodecDataInput cdi) {
    LocalDate date = decodeDate(flag, cdi);

    if (date == null) {
      return null;
    }
    return new Date(date.toDate().getTime());
  }
}
