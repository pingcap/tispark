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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.sql.Date;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DateType extends AbstractDateTimeType {
  public static final DateType DATE = new DateType(MySQLType.TypeDate);
  public static final MySQLType[] subTypes = new MySQLType[] { MySQLType.TypeDate };

  private DateType(MySQLType tp) {
    super(tp);
  }

  DateType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected DateTimeZone getTimezone() {
    return Converter.getLocalTimezone();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Date decodeNotNull(int flag, CodecDataInput cdi) {
    DateTime dateTime = decodeDateTime(flag, cdi);
    if (dateTime == null) {
      return null;
    }
    return new Date(dateTime.getMillis());
  }
}
