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

import static com.pingcap.tikv.types.TimeType.HOUR;
import static com.pingcap.tikv.types.TimeType.MICROSECOND;
import static com.pingcap.tikv.types.TimeType.MINUTE;
import static com.pingcap.tikv.types.TimeType.SECOND;
import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.exception.TypeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Converter {
  public static long convertToLong(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number) val).longValue();
    } else if (val instanceof String) {
      return Long.parseLong(val.toString());
    }
    throw new TypeException(
        String.format("Cannot cast %s to long", val.getClass().getSimpleName()));
  }

  public static double convertToDouble(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    } else if (val instanceof String) {
      return Double.parseDouble(val.toString());
    }
    throw new TypeException(
        String.format("Cannot cast %s to double", val.getClass().getSimpleName()));
  }

  public static String convertToString(Object val) {
    requireNonNull(val, "val is null");
    return val.toString();
  }

  public static byte[] convertToBytes(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return (byte[]) val;
    } else if (val instanceof String) {
      return ((String) val).getBytes();
    }
    throw new TypeException(
        String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  static byte[] convertToBytes(Object val, int prefixLength) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return Arrays.copyOf((byte[]) val, prefixLength);
    } else if (val instanceof String) {
      return Arrays.copyOf(((String) val).getBytes(), prefixLength);
    }
    throw new TypeException(
        String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  static byte[] convertUtf8ToBytes(Object val, int prefixLength) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return new String((byte[]) val).substring(0, prefixLength).getBytes(StandardCharsets.UTF_8);
    } else if (val instanceof String) {
      return ((String) val).substring(0, prefixLength).getBytes(StandardCharsets.UTF_8);
    }
    throw new TypeException(
        String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  private static final DateTimeZone localTimeZone = DateTimeZone.getDefault();
  private static final DateTimeFormatter localDateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(localTimeZone);
  private static final DateTimeFormatter localDateFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd").withZone(localTimeZone);
  public static final DateTimeFormatter UTC_TIME_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);

  public static DateTimeZone getLocalTimezone() {
    return localTimeZone;
  }

  public static DateTime strToDateTime(String value, DateTimeFormatter formatter) {
    return DateTime.parse(value, formatter);
  }

  /**
   * Convert an object to Datetime If constant is a string, it parses as local timezone If it is an
   * long, it parsed as UTC epoch
   *
   * @param val value to be converted to DateTime
   * @return joda.time.DateTime indicating local Datetime
   */
  public static DateTime convertToDateTime(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof DateTime) {
      return (DateTime) val;
    } else if (val instanceof String) {
      // interpret string as in local timezone
      try {
        return strToDateTime((String) val, localDateTimeFormatter);
      } catch (Exception e) {
        throw new TypeException(
            String.format("Error parsing string %s to datetime", (String) val), e);
      }
    } else if (val instanceof Long) {
      return new DateTime((long) val);
    } else if (val instanceof Timestamp) {
      return new DateTime(((Timestamp) val).getTime());
    } else if (val instanceof Date) {
      return new DateTime(((Date) val).getTime());
    } else {
      throw new TypeException("Can not cast Object to LocalDateTime ");
    }
  }

  /**
   * Convert an object to Date If constant is a string, it parses as local timezone If it is an
   * long, it parsed as UTC epoch
   *
   * @param val value to be converted to DateTime
   * @return java.sql.Date indicating Date
   */
  public static Date convertToDate(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Date) {
      return (Date) val;
    } else if (val instanceof String) {
      try {
        return new Date(DateTime.parse((String) val, localDateFormatter).toDate().getTime());
      } catch (Exception e) {
        throw new TypeException(String.format("Error parsing string %s to date", (String) val), e);
      }
    } else if (val instanceof Long) {
      return new Date((long) val);
    } else if (val instanceof Timestamp) {
      return new Date(((Timestamp) val).getTime());
    } else if (val instanceof DateTime) {
      return new Date(((DateTime) val).getMillis());
    } else {
      throw new TypeException("Can not cast Object to LocalDate");
    }
  }

  public static BigDecimal convertToBigDecimal(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof BigDecimal) {
      return (BigDecimal) val;
    } else if (val instanceof Double || val instanceof Float) {
      return new BigDecimal((Double) val);
    } else if (val instanceof BigInteger) {
      return new BigDecimal((BigInteger) val);
    } else if (val instanceof Number) {
      return new BigDecimal(((Number) val).longValue());
    } else if (val instanceof String) {
      return new BigDecimal((String) val);
    } else {
      throw new TypeException("can not cast non Number type to Double");
    }
  }

  public static String convertDurationToStr(long nanos, int decimal) {
    int sign = 1, hours, minutes, seconds, frac;
    if (nanos < 0) {
      nanos = -nanos;
      sign = -1;
    }
    hours = (int) (nanos / HOUR);
    nanos -= hours * HOUR;
    minutes = (int) (nanos / MINUTE);
    nanos -= minutes * MINUTE;
    seconds = (int) (nanos / SECOND);
    nanos -= seconds * SECOND;
    frac = (int) (nanos / MICROSECOND);
    StringBuilder sb = new StringBuilder();
    if (sign < 0) {
      sb.append('-');
    }
    sb.append(String.format("%02d:%02d:%02d", hours, minutes, seconds));
    if (decimal > 0) {
      sb.append('.');
      sb.append(String.format("%06d", frac), 0, decimal);
    }
    return sb.toString();
  }

  public static long convertStrToDuration(String value) {
    // value should be in form of 12:59:59.000 or 12:59:59
    // length expect to be 3.
    try {
      String[] splitBySemiColon = value.split(":");
      if (splitBySemiColon.length != 3)
        throw new IllegalArgumentException(
            String.format("%s is not a valid time type in mysql", value));
      int sign, hour, minute, second, frac;
      sign = 1;
      hour = Integer.parseInt(splitBySemiColon[0]);
      if (hour < 0) {
        sign = -1;
        hour -= hour;
      }
      minute = Integer.parseInt(splitBySemiColon[1]);
      if (splitBySemiColon[2].contains(".")) {
        String[] splitByDot = splitBySemiColon[2].split("\\.");
        second = Integer.parseInt(splitByDot[0]);
        frac = Integer.parseInt(splitByDot[1]);
      } else {
        second = Integer.parseInt(splitBySemiColon[2]);
        frac = 0;
      }
      return ((long) hour * HOUR
              + (long) minute * MINUTE
              + (long) second * SECOND
              + (long) frac * MICROSECOND)
          * sign;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not a valid format. Either hh:mm:ss.mmm or hh:mm:ss is accepted.", value));
    }
  }
}
