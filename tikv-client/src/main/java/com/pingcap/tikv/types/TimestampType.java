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
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TimestampType extends DataType {
  private static final ZoneId UTC_TIMEZONE = ZoneId.of("UTC");
  static TimestampType of(int tp) {
    return new TimestampType(tp);
  }

  protected ZoneId getDefaultTimezone() {
    return UTC_TIMEZONE;
  }

  private String getClassName() {
    return getClass().getSimpleName();
  }

  TimestampType(int tp) {
    super(tp);
  }

  TimestampType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == UVARINT_FLAG) {
      // read packedUInt
      LocalDateTime localDateTime = fromPackedLong(IntegerType.readUVarLong(cdi));
      if (localDateTime == null) {
        return null;
      }
      return Timestamp.from(ZonedDateTime.of(localDateTime, getDefaultTimezone()).toInstant());
    } else if (flag == UINT_FLAG) {
      // read UInt
      LocalDateTime localDateTime = fromPackedLong(IntegerType.readULong(cdi));
      if (localDateTime == null) {
        return null;
      }
      return Timestamp.from(ZonedDateTime.of(localDateTime, getDefaultTimezone()).toInstant());
    } else {
      throw new InvalidCodecFormatException("Invalid Flag type for " + getClassName() + ": " + flag);
    }
  }

  /**
   * encode a value to cdo per type.
   *
   * @param cdo destination of data.
   * @param encodeType Key or Value.
   * @param value need to be encoded.
   */
  @Override
  public void encodeNotNull(CodecDataOutput cdo, EncodeType encodeType, Object value) {
    LocalDateTime localDateTime;
    if (value instanceof LocalDateTime) {
      localDateTime = (LocalDateTime) value;
    } else {
      throw new UnsupportedOperationException("Can not cast Object to LocalDateTime ");
    }
    long val = toPackedLong(localDateTime);
    IntegerType.writeULongFull(cdo, val, false);
  }

  /**
   * get origin value from string
   * @param value a timestamp value in string in format "yyyy-MM-dd HH:mm:ss"
   * @return a {@link LocalDateTime} Object
   * TODO: need decode time with time zone info.
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    return LocalDateTime.parse(value, dateTimeFormatter);
  }

  /**
   * Encode a LocalDateTime to a packed long.
   *
   * @param time localDateTime that need to be encoded.
   * @return a packed long.
   */
  public static long toPackedLong(LocalDateTime time) {
    return toPackedLong(time.getYear(),
        time.getMonthValue(),
        time.getDayOfMonth(),
        time.getHour(),
        time.getMinute(),
        time.getSecond(),
        time.getNano() / 1000);
  }

  /**
   * Encode a date/time parts to a packed long.
   *
   * @return a packed long.
   */
  public static long toPackedLong(int year, int month, int day, int hour, int minute, int second, int micro) {
    long ymd = (year * 13 + month) << 5 | day;
    long hms = hour << 12 | minute << 6 | second;
    return ((ymd << 17 | hms) << 24) | micro;
  }

  /**
   * Encode a Date to a packed long with all time fields zero.
   *
   * @param date Date object that need to be encoded.
   * @return a packed long.
   */
  public static long toPackedLong(Date date) {
    return toPackedLong(
        date.getYear() + 1900,
        date.getMonth() + 1,
        date.getDate(),
        0, 0, 0, 0);
  }

  /**
   * Decode a packed long to LocalDateTime.
   *
   * @param packed a long value
   * @return a decoded LocalDateTime.
   */
  public static LocalDateTime fromPackedLong(long packed) {
    // TODO: As for JDBC behavior, it can be configured to "round" or "toNull"
    // for now we didn't pass in session so we do a toNull behavior
    if (packed == 0) {
      return null;
    }
    long ymdhms = packed >> 24;
    long ymd = ymdhms >> 17;
    int day = (int) (ymd & ((1 << 5) - 1));
    long ym = ymd >> 5;
    int month = (int) (ym % 13);
    int year = (int) (ym / 13);

    int hms = (int) (ymdhms & ((1 << 17) - 1));
    int second = hms & ((1 << 6) - 1);
    int minute = (hms >> 6) & ((1 << 6) - 1);
    int hour = hms >> 12;
    int microsec = (int) (packed % (1 << 24));
    return LocalDateTime.of(year, month, day, hour, minute, second, microsec * 1000);
  }
}
