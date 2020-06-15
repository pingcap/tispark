/*
 *
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tikv;

import java.sql.Timestamp;
import org.joda.time.DateTime;

/** Extend joda DateTime to support micro second */
public class ExtendedDateTime {

  private final DateTime dateTime;
  private final int microsOfMillis;

  /**
   * if timestamp = 2019-11-11 11:11:11 123456, then dateTime = 2019-11-11 11:11:11 123
   * microInMillis = 456
   *
   * @param dateTime
   * @param microsOfMillis
   */
  public ExtendedDateTime(DateTime dateTime, int microsOfMillis) {
    this.dateTime = dateTime;
    this.microsOfMillis = microsOfMillis;
  }

  public ExtendedDateTime(DateTime dateTime) {
    this.dateTime = dateTime;
    this.microsOfMillis = 0;
  }

  public DateTime getDateTime() {
    return dateTime;
  }

  public int getMicrosOfSeconds() {
    return dateTime.getMillisOfSecond() * 1000 + microsOfMillis;
  }

  public int getMicrosOfMillis() {
    return microsOfMillis;
  }

  public Timestamp toTimeStamp() {
    Timestamp timestamp = new Timestamp(dateTime.getMillis() / 1000 * 1000);
    timestamp.setNanos(dateTime.getMillisOfSecond() * 1000000 + microsOfMillis * 1000);
    return timestamp;
  }

  public long toEpochMicro() {
    return toTimeStamp().getTime() * 1000 + getMicrosOfMillis();
  }
}
