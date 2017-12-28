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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import org.junit.Test;

public class TimestampTest {
  @Test
  public void fromPackedLongAndToPackedLongTest() throws ParseException {

    LocalDateTime time = LocalDateTime.of(1999, 12, 12, 1, 1, 1, 1000);
    LocalDateTime time1 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time));
    assertEquals(time, time1);

    // since precision is microseconds, any nanoseconds is smaller than 1000 will be dropped.
    time = LocalDateTime.of(1999, 12, 12, 1, 1, 1, 1);
    time1 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time));
    assertNotEquals(time, time1);

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss:SSSSSSS");
    LocalDateTime time2 = LocalDateTime.parse("2010-10-10 10:11:11:0000000", formatter);
    LocalDateTime time3 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time2));
    assertEquals(time2, time3);

    // when packedLong is 0, then null is returned
    LocalDateTime time4 = TimestampType.fromPackedLong(0);
    assertNull(time4);

    LocalDateTime time5 = LocalDateTime.parse("9999-12-31 23:59:59:0000000", formatter);
    LocalDateTime time6 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time5));
    assertEquals(time5, time6);

    LocalDateTime time7 = LocalDateTime.parse("1000-01-01 00:00:00:0000000", formatter);
    LocalDateTime time8 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time7));
    assertEquals(time7, time8);

    LocalDateTime time9 = LocalDateTime.parse("2017-01-05 23:59:59:5756010", formatter);
    LocalDateTime time10 = TimestampType.fromPackedLong(TimestampType.toPackedLong(time9));
    assertEquals(time9, time10);

    SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd");
    Date date1 = formatter1.parse("2099-10-30");
    long time11 = TimestampType.toPackedLong(date1);
    LocalDateTime time12 = TimestampType.fromPackedLong(time11);
    assertEquals(time12.toLocalDate(), new java.sql.Date(date1.getTime()).toLocalDate());
  }
}
