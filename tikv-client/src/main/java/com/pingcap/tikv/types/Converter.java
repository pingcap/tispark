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

import com.pingcap.tikv.exception.TypeException;
import org.apache.spark.unsafe.types.UTF8String;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;

import static java.util.Objects.requireNonNull;

public class Converter {
  public static long convertToLong(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number)val).longValue();
    } else if (val instanceof String) {
      return Long.parseLong(val.toString());
    }
    throw new TypeException(String.format("Cannot cast %s to long", val.getClass().getSimpleName()));
  }

  public static double convertToDouble(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof Number) {
      return ((Number) val).doubleValue();
    } else if (val instanceof String) {
      return Double.parseDouble(val.toString());
    }
    throw new TypeException(String.format("Cannot cast %s to double", val.getClass().getSimpleName()));
  }

  public static String convertToString(Object val) {
    requireNonNull(val, "val is null");
    return val.toString();
  }

  public static byte[] convertToBytes(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return (byte[])val;
    } else if (val instanceof String) {
      return ((String) val).getBytes();
    }
    throw new TypeException(String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  static byte[] convertToBytes(Object val, int prefixLength) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return Arrays.copyOf((byte[])val, prefixLength);
    } else if (val instanceof String) {
      return Arrays.copyOf(((String) val).getBytes(), prefixLength);
    }
    throw new TypeException(String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  static byte[] convertUtf8ToBytes(Object val, int prefixLength) {
    requireNonNull(val, "val is null");
    if (val instanceof byte[]) {
      return UTF8String.fromBytes(((byte[]) val)).substring(0, prefixLength).getBytes();
    } else if (val instanceof String) {
      return UTF8String.fromString(((String) val)).substring(0, prefixLength).getBytes();
    }
    throw new TypeException(String.format("Cannot cast %s to bytes", val.getClass().getSimpleName()));
  }

  private static final DateTimeZone localTimeZone = DateTimeZone.getDefault();
  private static final DateTimeFormatter localDateTimeFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(localTimeZone);
  private static final DateTimeFormatter localDateFormatter =
      DateTimeFormat.forPattern("yyyy-MM-dd").withZone(localTimeZone);

  public static DateTimeZone getLocalTimezone() {
    return localTimeZone;
  }

  /**
   * Convert an object to Datetime
   * If constant is a string, it parses as local timezone
   * If it is an long, it parsed as UTC epoch
   * @param val value to be converted to DateTime
   * @return joda.time.DateTime indicating local Datetime
   */
  public static DateTime convertToDateTime(Object val) {
    requireNonNull(val, "val is null");
    if (val instanceof DateTime) {
      return (DateTime)val;
    } else if (val instanceof String) {
      // interpret string as in local timezone
      try {
        return DateTime.parse((String) val, localDateTimeFormatter);
      } catch (Exception e) {
        throw new TypeException(String.format("Error parsing string %s to datetime", (String)val), e);
      }
    } else if (val instanceof Long) {
      return new DateTime((long)val);
    } else if (val instanceof Timestamp) {
      return new DateTime(((Timestamp)val).getTime());
    } else if (val instanceof Date) {
      return new DateTime(((Date) val).getTime());
    } else {
      throw new TypeException("Can not cast Object to LocalDateTime ");
    }
  }

  /**
   * Convert an object to Date
   * If constant is a string, it parses as local timezone
   * If it is an long, it parsed as UTC epoch
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
      return new BigDecimal((Double)val);
    } else if (val instanceof BigInteger) {
      return new BigDecimal((BigInteger)val);
    } else if (val instanceof Number) {
      return new BigDecimal(((Number)val).longValue());
    } else if (val instanceof String) {
      return new BigDecimal((String)val);
    } else {
      throw new TypeException("can not cast non Number type to Double");
    }
  }
}
