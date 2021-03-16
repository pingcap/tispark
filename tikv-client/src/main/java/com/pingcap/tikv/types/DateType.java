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

  /**
   * In Spark 3.0, Proleptic Gregorian calendar is used in parsing, formatting, and converting dates
   * and timestamps as well as in extracting sub-components like years, days and so on. Spark 3.0
   * uses Java 8 API classes from the java.time packages that are based on ISO chronology. In Spark
   * version 2.4 and below, those operations are performed using the hybrid calendar (Julian +
   * Gregorian. The changes impact on the results for dates before October 15, 1582 (Gregorian) and
   * affect on the following Spark 3.0 API.
   *
   * @param d
   * @return
   */
  public int getDaysUsingJulianGregorianCalendar(LocalDate d) {
    // count how many days from EPOCH
    int days = Days.daysBetween(EPOCH, d).getDays();

    //  ~100-02-28 => -2
    // 100-03-01 ~200-02-28 => -1
    // 200-03-01 ~300-02-28 => +0
    // 300-03-01 ~500-02-28 => +1
    // 500-03-01 ~600-02-28 => +2
    // 600-03-01 ~700-02-28 => +3
    // 700-03-01 ~900-02-28 => +4
    // 900-03-01 ~1000-02-28 => +5
    // 1000-03-01 ~1100-02-28 => +6
    // 1100-03-01 ~ 1300-02-28 => +7
    // 1300-02-28 ~ 1400-02-28 => +8
    // 1400-03-01 ~ 1500-02-28 => +9
    // 1500-03-01 ~ 1582-10-14 => +10
    if(days < -141426) {
      if(days < -682943) {
        days = days - 2;
      } else if(days < -646419) {
        days = days - 1;
      } else if(days < -609895) {
        //days = days;
      } else if(days < -536846) {
        days = days + 1;
      } else if(days < -500322) {
        days = days + 2;
      } else if(days < -463798) {
        days = days + 3;
      } else if(days < -390749) {
        days = days + 4;
      } else if(days <-354225) {
        days = days + 5;
      } else if(days <-317701) {
        days = days + 6;
      } else if(days < -244652) {
        days = days + 7;
      } else if(days < -208128) {
        days = days + 8;
      } else if(days < -171604) {
        days = days + 9;
      } else if (days < -141426) {
        days = days + 10;
      }
    }

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

    return (long) getDaysUsingJulianGregorianCalendar(date);
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
