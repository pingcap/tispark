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

import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

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
  public static final TimestampType TIME = new TimestampType(MySQLType.TypeDuration);

  public static final MySQLType[] subTypes =
      new MySQLType[] {MySQLType.TypeTimestamp, MySQLType.TypeDuration};

  TimestampType(MySQLType tp) {
    super(tp);
  }

  TimestampType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  protected DateTimeZone getTimezone() {
    return DateTimeZone.UTC;
  }

  /**
   * Decode timestamp from packed long value In TiDB / MySQL, timestamp type is converted to UTC and
   * stored
   */
  @Override
  protected Timestamp decodeNotNull(int flag, CodecDataInput cdi) {
    DateTime dateTime = decodeDateTime(flag, cdi);
    if (dateTime == null) {
      return null;
    }
    return new Timestamp(dateTime.getMillis());
  }

  @Override
  public DateTime getOriginDefaultValueNonNull(String value) {
    return Converter.convertToDateTime(value);
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    DateTime dt = Converter.convertToDateTime(value);
    DateTimeCodec.writeDateTimeProto(cdo, dt, Converter.getLocalTimezone());
  }
}
