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

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.ExtendedDateTime;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.key.Handle;
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

  static final DateTimeFormatter UTC_TIME_FORMATTER =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(DateTimeZone.UTC);
  private static final DateTimeZone localTimeZone = DateTimeZone.getDefault();
  private static final DateTimeFormatter localDateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(localTimeZone);
  private static final DateTimeFormatter localDateFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd").withZone(localTimeZone);

  public static Long safeConvertToSigned(Object value, Long lowerBound, Long upperBound)
      throws ConvertNotSupportException, ConvertOverflowException {
    Long result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = 1L;
      } else {
        result = 0L;
      }
    } else if (value instanceof Byte) {
      result = ((Byte) value).longValue();
    } else if (value instanceof Short) {
      result = ((Short) value).longValue();
    } else if (value instanceof Integer) {
      result = ((Integer) value).longValue();
    } else if (value instanceof Long) {
      result = (Long) value;
    } else if (value instanceof Float) {
      result = floatToLong((Float) value);
    } else if (value instanceof Double) {
      result = doubleToLong((Double) value);
    } else if (value instanceof String) {
      result = stringToLong((String) value);
    } else if (value instanceof java.math.BigDecimal) {
      result = ((java.math.BigDecimal) value).longValueExact();
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), "SIGNED");
    }

    if (result < lowerBound) {
      throw ConvertOverflowException.newLowerBoundException(result, lowerBound);
    }

    if (result > upperBound) {
      throw ConvertOverflowException.newUpperBoundException(result, upperBound);
    }
    return result;
  }

  public static Long safeConvertToUnsigned(Object value, Long upperBound)
      throws ConvertNotSupportException, ConvertOverflowException {
    Long result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = 1L;
      } else {
        result = 0L;
      }
    } else if (value instanceof Byte) {
      result = ((Byte) value).longValue();
    } else if (value instanceof Short) {
      result = ((Short) value).longValue();
    } else if (value instanceof Integer) {
      result = ((Integer) value).longValue();
    } else if (value instanceof Long) {
      result = (Long) value;
    } else if (value instanceof Float) {
      result = floatToLong((Float) value);
    } else if (value instanceof Double) {
      result = doubleToLong((Double) value);
    } else if (value instanceof String) {
      UnsignedLong unsignedLong = stringToUnsignedLong((String) value);
      result = unsignedLong.longValue();
    } else if (value instanceof java.math.BigDecimal) {
      result = ((java.math.BigDecimal) value).longValueExact();
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), "UNSIGNED");
    }

    long lowerBound = 0L;
    if (java.lang.Long.compareUnsigned(result, lowerBound) < 0) {
      throw ConvertOverflowException.newLowerBoundException(result, lowerBound);
    }

    if (java.lang.Long.compareUnsigned(result, upperBound) > 0) {
      throw ConvertOverflowException.newUpperBoundException(result, upperBound);
    }
    return result;
  }

  public static Long floatToLong(Float v) {
    return (long) Math.round(v);
  }

  public static Long doubleToLong(Double v) {
    return Math.round(v);
  }

  public static Long stringToLong(String v) {
    return Long.parseLong(v);
  }

  public static Double stringToDouble(String v) {
    return Double.parseDouble(v);
  }

  public static UnsignedLong stringToUnsignedLong(String v) {
    return UnsignedLong.valueOf(v);
  }

  public static long convertToLong(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number) val).longValue();
    } else if (val instanceof String) {
      return Long.parseLong(val.toString());
    } else if (val instanceof Handle) {
      return ((Handle) val).intValue();
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
      byte[] valByte = (byte[]) val;
      return Arrays.copyOf(valByte, Math.min(valByte.length, prefixLength));
    } else if (val instanceof String) {
      String valStr = (String) val;
      return Arrays.copyOf(((String) val).getBytes(), Math.min(valStr.length(), prefixLength));
    }
    throw new TypeException(
        String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  static byte[] convertUtf8ToBytes(Object val, int prefixLength) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      byte[] valByte = (byte[]) val;
      return new String(valByte)
          .substring(0, Math.min(valByte.length, prefixLength))
          .getBytes(StandardCharsets.UTF_8);
    } else if (val instanceof String) {
      String valStr = (String) val;
      return valStr
          .substring(0, Math.min(valStr.length(), prefixLength))
          .getBytes(StandardCharsets.UTF_8);
    }
    throw new TypeException(
        String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  public static DateTimeZone getLocalTimezone() {
    return localTimeZone;
  }

  static DateTime strToDateTime(String value, DateTimeFormatter formatter) {
    return DateTime.parse(value, formatter);
  }

  /**
   * Convert an object to Datetime. If constant is a string, it parses as local timezone. If it is
   * an long, it parsed as UTC epoch. If it is a TimeStamp, it parses as local timezone.
   *
   * @param val value to be converted to DateTime
   * @return joda.time.DateTime indicating local Datetime
   */
  public static ExtendedDateTime convertToDateTime(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof DateTime) {
      return new ExtendedDateTime((DateTime) val);
    } else if (val instanceof String) {
      // interpret string as in local timezone
      try {
        return new ExtendedDateTime(strToDateTime((String) val, localDateTimeFormatter));
      } catch (Exception e) {
        throw new TypeException(String.format("Error parsing string %s to datetime", val), e);
      }
    } else if (val instanceof Long) {
      return new ExtendedDateTime(new DateTime((long) val));
    } else if (val instanceof Timestamp) {
      Timestamp timestamp = (Timestamp) val;
      DateTime dateTime = new DateTime(timestamp.getTime());
      int nanos = timestamp.getNanos();
      return new ExtendedDateTime(dateTime, (nanos / 1000) % 1000);
    } else if (val instanceof Date) {
      return new ExtendedDateTime(new DateTime(((Date) val).getTime()));
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
        throw new TypeException(String.format("Error parsing string %s to date", val), e);
      }
    } else if (val instanceof Integer) {
      // when the val is a Integer, it is only have year part of a Date.
      return new Date((Integer) val, 0, 0);
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
