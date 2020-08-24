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

import static com.pingcap.tikv.types.Converter.UTC_TIME_FORMATTER;

import com.pingcap.tikv.ExtendedDateTime;
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDateTime;

/**
 * Timestamp in TiDB is represented as packed long including year/month and etc. When stored, it is
 * converted into UTC and when read it should be interpreted as UTC and convert to local When
 * encoded as coprocessor request 1. all date time in key should be converted to UTC 2. all date
 * time in proto should be converted into local timezone 3. all incoming data should be regarded as
 * local timezone if no timezone indicated
 *
 * <p>For example, Encoding: If incoming literal is a string '2000-01-01 00:00:00' which has no
 * timezone, it interpreted as text in local timezone and encoded with UTC; If incoming literal is a
 * epoch millisecond, it interpreted as UTC epoch and encode with UTC if index / key or local
 * timezone if proto
 */
public class TimestampType extends AbstractDateTimeType {
  public static final TimestampType TIMESTAMP = new TimestampType(MySQLType.TypeTimestamp);

  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeTimestamp};

  TimestampType(MySQLType tp) {
    super(tp);
  }

  TimestampType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  public DateTimeZone getTimezone() {
    return DateTimeZone.UTC;
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToMysqlLocalTimestamp(value);
  }

  private java.sql.Timestamp convertToMysqlLocalTimestamp(Object value)
      throws ConvertNotSupportException {
    java.sql.Timestamp result;
    if (value instanceof Long) {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
      // result = new java.sql.Timestamp((Long) value);
    } else {
      return convertToMysqlDateTime(value);
    }
  }

  /**
   * Decode timestamp from packed long value In TiDB / MySQL, timestamp type is converted to UTC and
   * stored
   */
  @Override
  protected Long decodeNotNull(int flag, CodecDataInput cdi) {
    return decodeDateTime(flag, cdi);
  }

  @Override
  protected Timestamp decodeNotNullForBatchWrite(int flag, CodecDataInput cdi) {
    return decodeDateTimeForBatchWrite(flag, cdi);
  }

  @Override
  public DateTime getOriginDefaultValueNonNull(String value, long version) {
    if (version >= DataType.COLUMN_VERSION_FLAG) {
      LocalDateTime localDateTime =
          Converter.strToDateTime(value, UTC_TIME_FORMATTER)
              .withZone(DateTimeZone.getDefault())
              .toLocalDateTime();
      return localDateTime.toDateTime();
    }
    return Converter.convertToDateTime(value).getDateTime();
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    ExtendedDateTime localExtendedDateTime = Converter.convertToDateTime(value);
    DateTime utcDateTime = localExtendedDateTime.getDateTime().toDateTime(DateTimeZone.UTC);
    ExtendedDateTime utcExtendedDateTime =
        new ExtendedDateTime(utcDateTime, localExtendedDateTime.getMicrosOfMillis());
    DateTimeCodec.writeDateTimeProto(cdo, utcExtendedDateTime, Converter.getLocalTimezone());
  }

  @Override
  public String getName() {
    return "TIMESTAMP";
  }
}
