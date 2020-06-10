/*
 * Copyright 2020 PingCAP, Inc.
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
 */

package com.pingcap.tikv.columnar;

/**
 * TiCoreTime is TiDB's representation of date/datetime/timestamp, used to decode chunk data from
 * dag response
 */
public class TiCoreTime {
  // copied from https://github.com/pingcap/tidb/blob/master/types/time.go
  private static final long YEAR_BIT_FIELD_OFFSET = 50, YEAR_BIT_FIELD_WIDTH = 14;
  private static final long MONTH_BIT_FIELD_OFFSET = 46, MONTH_BIT_FIELD_WIDTH = 4;
  private static final long DAY_BIT_FIELD_OFFSET = 41, DAY_BIT_FIELD_WIDTH = 5;
  private static final long HOUR_BIT_FIELD_OFFSET = 36, HOUR_BIT_FIELD_WIDTH = 5;
  private static final long MINUTE_BIT_FIELD_OFFSET = 30, MINUTE_BIT_FIELD_WIDTH = 6;
  private static final long SECOND_BIT_FIELD_OFFSET = 24, SECOND_BIT_FIELD_WIDTH = 6;
  private static final long MICROSECOND_BIT_FIELD_OFFSET = 4, MICROSECOND_BIT_FIELD_WIDTH = 20;
  private static final long YEAR_BIT_FIELD_MASK =
      ((1L << YEAR_BIT_FIELD_WIDTH) - 1) << YEAR_BIT_FIELD_OFFSET;
  private static final long MONTH_BIT_FIELD_MASK =
      ((1L << MONTH_BIT_FIELD_WIDTH) - 1) << MONTH_BIT_FIELD_OFFSET;
  private static final long DAY_BIT_FIELD_MASK =
      ((1L << DAY_BIT_FIELD_WIDTH) - 1) << DAY_BIT_FIELD_OFFSET;
  private static final long HOUR_BIT_FIELD_MASK =
      ((1L << HOUR_BIT_FIELD_WIDTH) - 1) << HOUR_BIT_FIELD_OFFSET;
  private static final long MINUTE_BIT_FIELD_MASK =
      ((1L << MINUTE_BIT_FIELD_WIDTH) - 1) << MINUTE_BIT_FIELD_OFFSET;
  private static final long SECOND_BIT_FIELD_MASK =
      ((1L << SECOND_BIT_FIELD_WIDTH) - 1) << SECOND_BIT_FIELD_OFFSET;
  private static final long MICROSECOND_BIT_FIELD_MASK =
      ((1L << MICROSECOND_BIT_FIELD_WIDTH) - 1) << MICROSECOND_BIT_FIELD_OFFSET;

  private final long coreTime;

  public TiCoreTime(long coreTime) {
    this.coreTime = coreTime;
  }

  public int getYear() {
    return (int) ((coreTime & YEAR_BIT_FIELD_MASK) >>> YEAR_BIT_FIELD_OFFSET);
  }

  public int getMonth() {
    return (int) ((coreTime & MONTH_BIT_FIELD_MASK) >>> MONTH_BIT_FIELD_OFFSET);
  }

  public int getDay() {
    return (int) ((coreTime & DAY_BIT_FIELD_MASK) >>> DAY_BIT_FIELD_OFFSET);
  }

  public int getHour() {
    return (int) ((coreTime & HOUR_BIT_FIELD_MASK) >>> HOUR_BIT_FIELD_OFFSET);
  }

  public int getMinute() {
    return (int) ((coreTime & MINUTE_BIT_FIELD_MASK) >>> MINUTE_BIT_FIELD_OFFSET);
  }

  public int getSecond() {
    return (int) ((coreTime & SECOND_BIT_FIELD_MASK) >>> SECOND_BIT_FIELD_OFFSET);
  }

  public long getMicroSecond() {
    return (coreTime & MICROSECOND_BIT_FIELD_MASK) >>> MICROSECOND_BIT_FIELD_OFFSET;
  }
}
