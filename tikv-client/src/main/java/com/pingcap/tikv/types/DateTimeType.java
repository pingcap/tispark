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

import com.pingcap.tikv.ExtendedDateTime;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Timestamp;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

/**
 * Datetime is a timezone neutral version of timestamp While most of decoding logic is the same it
 * interpret as local timezone to be able to compute with date/time data
 */
public class DateTimeType extends AbstractDateTimeType {
  public static final DateTimeType DATETIME = new DateTimeType(MySQLType.TypeDatetime);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeDatetime};

  private DateTimeType(MySQLType tp) {
    super(tp);
  }

  DateTimeType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected DateTimeZone getTimezone() {
    return Converter.getLocalTimezone();
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToMysqlDateTime(value);
  }

  /**
   * Decode timestamp from packed long value In TiDB / MySQL, timestamp type is converted to UTC and
   * stored
   */
  @Override
  protected Timestamp decodeNotNull(int flag, CodecDataInput cdi) {
    ExtendedDateTime extendedDateTime = decodeDateTime(flag, cdi);
    // Even though null is filtered out but data like 0000-00-00 exists
    // according to MySQL JDBC behavior, it's converted to null
    if (extendedDateTime == null) {
      return null;
    }
    return extendedDateTime.toTimeStamp();
  }

  @Override
  public DateTime getOriginDefaultValueNonNull(String value, long version) {
    return Converter.convertToDateTime(value).getDateTime();
  }
}
