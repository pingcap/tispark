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

package com.pingcap.tikv.types;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import org.junit.Test;

public class ConverterTest {

  @Test
  public void convertToLongTest() {}

  @Test
  public void convertToDoubleTest() {}

  @Test
  public void convertToStringTest() {}

  @Test
  public void convertToBytesTest() {
    String dairy = "dairy";
    byte[] dairyBytes = dairy.getBytes();
    assertArrayEquals(Converter.convertToBytes(dairy, 2), new byte[] {100, 97});
    assertArrayEquals(Converter.convertToBytes(dairy, 5), new byte[] {100, 97, 105, 114, 121});
    assertArrayEquals(Converter.convertToBytes(dairy, 10), new byte[] {100, 97, 105, 114, 121});
    assertArrayEquals(Converter.convertToBytes(dairyBytes, 2), new byte[] {100, 97});
    assertArrayEquals(Converter.convertToBytes(dairyBytes, 5), new byte[] {100, 97, 105, 114, 121});
    assertArrayEquals(
        Converter.convertToBytes(dairyBytes, 10), new byte[] {100, 97, 105, 114, 121});
  }

  @Test
  public void convertUtf8ToBytesTest() {
    String dairy = "dairy";
    byte[] dairyBytes = dairy.getBytes(StandardCharsets.UTF_8);
    assertArrayEquals(Converter.convertUtf8ToBytes(dairy, 2), new byte[] {100, 97});
    assertArrayEquals(Converter.convertUtf8ToBytes(dairy, 5), new byte[] {100, 97, 105, 114, 121});
    assertArrayEquals(Converter.convertUtf8ToBytes(dairy, 10), new byte[] {100, 97, 105, 114, 121});
    assertArrayEquals(Converter.convertUtf8ToBytes(dairyBytes, 2), new byte[] {100, 97});
    assertArrayEquals(
        Converter.convertUtf8ToBytes(dairyBytes, 5), new byte[] {100, 97, 105, 114, 121});
    assertArrayEquals(
        Converter.convertUtf8ToBytes(dairyBytes, 10), new byte[] {100, 97, 105, 114, 121});
  }

  @Test
  public void getLocalTimezoneTest() {}

  @Test
  public void convertToDateTimeTest() {}

  @Test
  public void convertToBigDecimalTest() {}

  @Test(expected = IllegalArgumentException.class)
  public void convertStrToDurationExceptionTest() {
    Converter.convertStrToDuration("");
    Converter.convertStrToDuration("12");
  }

  @Test
  public void convertStrToDurationTest() {
    assertEquals(Converter.convertStrToDuration("12:59:59"), 46799000000000L);
    assertEquals(Converter.convertStrToDuration("838:59:59"), 3020399000000000L);
  }

  @Test
  public void convertDurationToStrTest() {
    assertEquals(Converter.convertDurationToStr(46799000000000L, 3), "12:59:59.000");
    assertEquals(Converter.convertDurationToStr(46799000000000L, 0), "12:59:59");
    assertEquals(Converter.convertDurationToStr(46799000000000L, 4), "12:59:59.0000");
    assertEquals(Converter.convertDurationToStr(3020399000000000L, 4), "838:59:59.0000");
  }
}
