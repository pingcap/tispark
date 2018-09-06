/*
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
 */

package com.pingcap.tikv.codec;


import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.codec.Codec.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.TimeZone;

import static com.pingcap.tikv.codec.Codec.*;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.*;

public class CodecTest {
  @Test
  public void writeDoubleAndReadDoubleTest() {
    // issue scientific notation in toBin
    CodecDataOutput cdo = new CodecDataOutput();
    RealCodec.writeDouble(cdo, 0.01);
    double u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(0.01, u, 0.0001);

    cdo.reset();
    RealCodec.writeDouble(cdo, 206.0);
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(206.0, u, 0.0001);

    cdo.reset();
    RealCodec.writeDouble(cdo, 0);
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(0, u, 0.0001);

    cdo.reset();
    RealCodec.writeDouble(cdo, -0.01);
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(-0.01, u, 0.0001);

    cdo.reset();
    RealCodec.writeDouble(cdo, -206.0);
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(-206.0, u, 0.0001);
  }

  @Test
  public void readNWriteDecimalTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    DecimalCodec.writeDecimal(cdo, BigDecimal.valueOf(0));
    BigDecimal bigDec = DecimalCodec.readDecimal(new CodecDataInput(cdo.toBytes()));
    assertEquals(0, bigDec.doubleValue(), 0.0001);

    cdo.reset();
    DecimalCodec.writeDecimal(cdo, BigDecimal.valueOf(206.01));
    bigDec = DecimalCodec.readDecimal(new CodecDataInput(cdo.toBytes()));
    assertEquals(206.01, bigDec.doubleValue(), 0.0001);

    cdo.reset();
    DecimalCodec.writeDecimal(cdo, BigDecimal.valueOf(-206.01));
    bigDec = DecimalCodec.readDecimal(new CodecDataInput(cdo.toBytes()));
    assertEquals(-206.01, bigDec.doubleValue(), 0.0001);

    byte[] wrongData = new byte[] {(byte) 0x8};
    CodecDataInput cdi = new CodecDataInput(wrongData);
    try {
      DecimalCodec.readDecimal(cdi);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void readNWriteLongTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeLongFully(cdo, 9999L, true);
    IntegerCodec.writeLongFully(cdo, -2333L, false);
    assertArrayEquals(
        new byte[] {
            (byte) 0x3,
            (byte) 0x80,
            (byte) 0x0,
            (byte) 0x0,
            (byte) 0x0,
            (byte) 0x0,
            (byte) 0x0,
            (byte) 0x27,
            (byte) 0xf,
            (byte) 0x8,
            (byte) 0xb9,
            (byte) 0x24
        },
        cdo.toBytes());
    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(INT_FLAG, cdi.readByte());
    long value = IntegerCodec.readLong(cdi);
    assertEquals(9999L, value);
    assertEquals(VARINT_FLAG, cdi.readByte());
    value = IntegerCodec.readVarLong(cdi);
    assertEquals(-2333L, value);

    byte[] wrongData = new byte[] {(byte) 0x8, (byte) 0xb9};
    cdi = new CodecDataInput(wrongData);
    try {
      IntegerCodec.readLong(cdi);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void readNWriteUnsignedLongTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeULongFully(cdo, 0xffffffffffffffffL, true);
    IntegerCodec.writeULongFully(cdo, Long.MIN_VALUE, false);
    assertArrayEquals(
        new byte[] {
            (byte) 0x4,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0xff,
            (byte) 0x9,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x80,
            (byte) 0x1
        },
        cdo.toBytes());
    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());

    assertEquals(UINT_FLAG, cdi.readByte());
    long value = IntegerCodec.readULong(cdi);
    assertEquals(0xffffffffffffffffL, value);

    assertEquals(UVARINT_FLAG, cdi.readByte());
    value = IntegerCodec.readUVarLong(cdi);
    assertEquals(Long.MIN_VALUE, value);

    byte[] wrongData =
        new byte[] {
            (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0x80, (byte) 0x80
        };
    cdi = new CodecDataInput(wrongData);
    try {
      cdi.skipBytes(1);
      IntegerCodec.readUVarLong(cdi);
      fail();
    } catch (Exception e) {
      assertEquals("readUVarLong encountered unfinished data", e.getMessage());
    }

    // the following two tests are for overflow readUVarLong
    wrongData =
        new byte[] {
            (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0x80, (byte) 0x80, (byte) 0x02
        };
    cdi = new CodecDataInput(wrongData);
    try {
      cdi.skipBytes(1);
      IntegerCodec.readUVarLong(cdi);
      fail();
    } catch (Exception e) {
      assertEquals("readUVarLong overflow", e.getMessage());
    }

    wrongData =
        new byte[] {
            (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
            (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x01
        };
    cdi = new CodecDataInput(wrongData);
    try {
      cdi.skipBytes(1);
      IntegerCodec.readUVarLong(cdi);
      fail();
    } catch (Exception e) {
      assertEquals("readUVarLong overflow", e.getMessage());
    }
  }

  private static byte[] toBytes(int[] arr) {
    byte[] bytes = new byte[arr.length];
    for (int i = 0; i < arr.length; i++) {
      bytes[i] = (byte)(arr[i] & 0xFF);
    }
    return bytes;
  }

  @Test
  public void writeBytesTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    Codec.BytesCodec.writeBytes(cdo, "abcdefghijk".getBytes());
    byte[] result = cdo.toBytes();
    byte[] expected = toBytes(new int[]{97,98,99,100,101,102,103,104,255,105,106,107,0,0,0,0,0,250});
    assertArrayEquals(expected, result);

    cdo.reset();
    Codec.BytesCodec.writeBytes(cdo, "abcdefghijk".getBytes());
    result = BytesCodec.readBytes(new CodecDataInput(cdo.toBytes()));
    expected = toBytes(new int[]{97,98,99,100,101,102,103,104,105,106,107});
    assertArrayEquals(expected, result);

    cdo.reset();
    Codec.BytesCodec.writeBytes(cdo, "fYfSp".getBytes());
    result = cdo.toBytes();
    expected = toBytes(new int[]{102,89,102,83,112,0,0,0,252});
    assertArrayEquals(expected, result);

    cdo.reset();
    Codec.BytesCodec.writeBytesRaw(cdo, "fYfSp".getBytes());
    result = cdo.toBytes();
    expected = toBytes(new int[]{102,89,102,83,112});
    assertArrayEquals(expected, result);
  }

  @Test
  public void readBytesTest() {
    // TODO: How to test private
    byte[] data =
        new byte[] {
            (byte) 0x61,
            (byte) 0x62,
            (byte) 0x63,
            (byte) 0x64,
            (byte) 0x65,
            (byte) 0x66,
            (byte) 0x67,
            (byte) 0x68,
            (byte) 0xff,
            (byte) 0x69,
            (byte) 0x6a,
            (byte) 0x6b,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0xfa
        };
    CodecDataInput cdi = new CodecDataInput(data);
    // byte[] result = BytesCodec.readBytes(cdi, false);


  }

  @Test
  public void writeFloatTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    RealCodec.writeDouble(cdo, 0.00);
    double u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(0.00, u, 0);

    cdo.reset();
    RealCodec.writeDouble(cdo, Double.MAX_VALUE);
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(Double.MAX_VALUE, u, 0);

    cdo.reset();
    RealCodec.writeDouble(cdo, Double.MIN_VALUE);
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(Double.MIN_VALUE, u, 0);
  }

  @Test
  public void readFloatTest() throws Exception {
    byte[] data =
        new byte[] {
            (byte) (191 & 0xFF),
            (byte) (241 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (160 & 0xFF),
            0,
            0,
            0
        };
    CodecDataInput cdi = new CodecDataInput(data);
    double u = RealCodec.readDouble(cdi);
    assertEquals(1.1, u, 0.0001);

    data =
        new byte[] {
            (byte) (192 & 0xFF),
            (byte) (1 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (153 & 0xFF),
            (byte) (154 & 0xFF)
        };
    cdi = new CodecDataInput(data);
    u = RealCodec.readDouble(cdi);
    assertEquals(2.2, u, 0.0001);

    data =
        new byte[] {
            (byte) (63 & 0xFF),
            (byte) (167 & 0xFF),
            (byte) (51 & 0xFF),
            (byte) (67 & 0xFF),
            (byte) (159 & 0xFF),
            (byte) (0xFF),
            (byte) (0xFF),
            (byte) (0xFF)
        };

    cdi = new CodecDataInput(data);
    u = RealCodec.readDouble(cdi);
    assertEquals(-99.199, u, 0.0001);
  }

  @Test
  public void negativeLongTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerCodec.writeULong(cdo, UnsignedLong.valueOf("13831004815617530266").longValue());
    double u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(1.1, u, 0.001);

    cdo.reset();
    IntegerCodec.writeULong(cdo, UnsignedLong.valueOf("13835508415244900762").longValue());
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(2.2, u, 0.001);

    cdo.reset();
    IntegerCodec.writeULong(cdo, UnsignedLong.valueOf("13837985394932580352").longValue());
    u = RealCodec.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(3.3, u, 0.001);
  }

  @Test
  public void fromPackedLongAndToPackedLongTest() {
    DateTimeZone utc = DateTimeZone.UTC;
    DateTimeZone otherTz = DateTimeZone.forOffsetHours(-8);
    DateTime time = new DateTime(1999, 12, 12, 1, 1, 1, 999);
    // Encode as UTC (loss timezone) and read it back as UTC
    DateTime time1 = requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time, utc), utc));
    assertEquals(time.getMillis(), time1.getMillis());

    // Parse String as -8 timezone, encode and read it back
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSSSSSS").withZone(otherTz);
    DateTime time2 = DateTime.parse("2010-10-10 10:11:11:0000000", formatter);
    DateTime time3 = requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time2, otherTz), otherTz));
    assertEquals(time2.getMillis(), time3.getMillis());

    // when packedLong is 0, then null is returned
    DateTime time4 = DateTimeCodec.fromPackedLong(0, otherTz);
    assertNull(time4);

    DateTime time5 = DateTime.parse("9999-12-31 23:59:59:0000000", formatter);
    DateTime time6 = requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time5, otherTz), otherTz));
    assertEquals(time5.getMillis(), time6.getMillis());

    DateTime time7 = DateTime.parse("1000-01-01 00:00:00:0000000", formatter);
    DateTime time8 = requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time7, otherTz), otherTz));
    assertEquals(time7.getMillis(), time8.getMillis());

    DateTime time9 = DateTime.parse("2017-01-05 23:59:59:5756010", formatter);
    DateTime time10 = requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time9, otherTz), otherTz));
    assertEquals(time9.getMillis(), time10.getMillis());

    DateTimeFormatter formatter1 = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTime date1 = DateTime.parse("2099-10-30", formatter1);
    long time11 = DateTimeCodec.toPackedLong(date1, otherTz);
    DateTime time12 = requireNonNull(DateTimeCodec.fromPackedLong(time11, otherTz));
    assertEquals(time12.getMillis(), date1.getMillis());

    Calendar cal = Calendar.getInstance(otherTz.toTimeZone());
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long millis = cal.getTimeInMillis();
    DateTime time14 = requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(millis, otherTz), otherTz));
    assertEquals(millis, time14.getMillis());
  }

  @Test
  public void DSTTest() {
    DateTimeZone defaultDateTimeZone = DateTimeZone.getDefault();
    DateTimeZone utc = DateTimeZone.UTC;
    DateTimeZone dst = DateTimeZone.forID("America/New_York");
    TimeZone defaultTimeZone = TimeZone.getDefault();
    TimeZone utcTimeZone = TimeZone.getTimeZone("UTC");
    TimeZone dstTimeZone = TimeZone.getTimeZone("America/New_York");

    TimeZone.setDefault(utcTimeZone);
    DateTimeZone.setDefault(utc);

    DateTime time = new DateTime(2007, 3, 11, 2, 0, 0, 0);
    DateTime time11 = new DateTime(2007, 3, 11, 7, 0, 0, 0).toDateTime(dst);
    DateTime time21 = new DateTime(2007, 3, 11, 6, 0, 0, 0).toDateTime(dst);

    TimeZone.setDefault(dstTimeZone);
    DateTimeZone.setDefault(dst);

    // Local Time 2007-03-11T02:00:00.000 should be in dst gap
    assertTrue(dst.isLocalDateTimeGap(time.toLocalDateTime()));

    // Try Decode a dst datetime
    DateTime time12 = requireNonNull(DateTimeCodec.fromPackedLong(((((2007L * 13 + 3) << 5L | 11) << 17L) | (2 << 12)) << 24L, dst));
    DateTime time22 = requireNonNull(DateTimeCodec.fromPackedLong(((((2007L * 13 + 3) << 5L | 11) << 17L) | (1 << 12)) << 24L, dst));
    DateTime time23 = requireNonNull(DateTimeCodec.fromPackedLong(((((2007L * 13 + 3) << 5L | 11) << 17L) | (7 << 12)) << 24L, utc));
    DateTime time24 = time23.toDateTime(dst);
    // time11: 2007-03-11T03:00:00.000-04:00
    // time21: 2007-03-11T01:00:00.000-05:00
    // time12: 2007-03-11T03:00:00.000-04:00
    // time22: 2007-03-11T01:00:00.000-05:00
    // time23: 2007-03-11T07:00:00.000Z
    // time24: 2007-03-11T03:00:00.000-04:00
    assertEquals(time11.getMillis(), time12.getMillis());
    assertEquals(time21.getMillis(), time22.getMillis());
    assertEquals(time12.getMillis(), time23.getMillis());
    assertEquals(time23.getMillis(), time24.getMillis());
    assertEquals(time22.getMillis() + 60 * 60 * 1000, time23.getMillis());

    assertEquals(time12.toLocalDateTime().getHourOfDay(), 3);
    assertEquals(time22.toLocalDateTime().getHourOfDay(), 1);
    assertEquals(time23.toLocalDateTime().getHourOfDay(), 7);
    assertEquals(time24.toLocalDateTime().getHourOfDay(), 3);

    TimeZone.setDefault(defaultTimeZone);
    DateTimeZone.setDefault(defaultDateTimeZone);
  }

  @Test
  public void readNWriteDateTimeTest() {
    DateTimeZone otherTz = DateTimeZone.forOffsetHours(-8);
    DateTime time = new DateTime(2007, 3, 11, 2, 0, 0, 0);

    CodecDataOutput cdo = new CodecDataOutput();
    DateTimeCodec.writeDateTimeFully(cdo, time, otherTz);
    DateTimeCodec.writeDateTimeProto(cdo, time, otherTz);

    assertArrayEquals(
        new byte[] {
            (byte) 0x4,
            (byte) 0x19,
            (byte) 0x7b,
            (byte) 0x94,
            (byte) 0xa0,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x19,
            (byte) 0x7b,
            (byte) 0x94,
            (byte) 0xa0,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
            (byte) 0x00,
        },
        cdo.toBytes());

    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(UINT_FLAG, cdi.readByte());
    DateTime time2 = DateTimeCodec.readFromUInt(cdi, otherTz);
    assertEquals(time.getMillis(), time2.getMillis());
    time2 = DateTimeCodec.readFromUInt(cdi, otherTz);
    assertEquals(time.getMillis(), time2.getMillis());

    cdo.reset();
    IntegerCodec.writeULongFully(cdo, DateTimeCodec.toPackedLong(time, otherTz), false);
    cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(UVARINT_FLAG, cdi.readByte());
    time2 = DateTimeCodec.readFromUVarInt(cdi, otherTz);
    assertEquals(time.getMillis(), time2.getMillis());
  }
}