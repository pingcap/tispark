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

import static com.pingcap.tikv.codec.Codec.INT_FLAG;
import static com.pingcap.tikv.codec.Codec.UINT_FLAG;
import static com.pingcap.tikv.codec.Codec.UVARINT_FLAG;
import static com.pingcap.tikv.codec.Codec.VARINT_FLAG;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.ExtendedDateTime;
import com.pingcap.tikv.codec.Codec.BytesCodec;
import com.pingcap.tikv.codec.Codec.DateCodec;
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.Codec.DecimalCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.Codec.RealCodec;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.Calendar;
import java.util.TimeZone;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

public class CodecTest {
  private static byte[] toBytes(int[] arr) {
    byte[] bytes = new byte[arr.length];
    for (int i = 0; i < arr.length; i++) {
      bytes[i] = (byte) (arr[i] & 0xFF);
    }
    return bytes;
  }

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
  public void readNWriteDecimalTest() {
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
  public void readNWriteLongTest() {
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
  public void readNWriteUnsignedLongTest() {
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

  @Test
  public void writeBytesTest() {
    CodecDataOutput cdo = new CodecDataOutput();
    BytesCodec.writeBytes(cdo, "abcdefghijk".getBytes());
    byte[] result = cdo.toBytes();
    byte[] expected =
        toBytes(
            new int[] {
              97, 98, 99, 100, 101, 102, 103, 104, 255, 105, 106, 107, 0, 0, 0, 0, 0, 250
            });
    assertArrayEquals(expected, result);

    cdo.reset();
    BytesCodec.writeBytes(cdo, "abcdefghijk".getBytes());
    result = BytesCodec.readBytes(new CodecDataInput(cdo.toBytes()));
    expected = toBytes(new int[] {97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107});
    assertArrayEquals(expected, result);

    cdo.reset();
    BytesCodec.writeBytes(cdo, "fYfSp".getBytes());
    result = cdo.toBytes();
    expected = toBytes(new int[] {102, 89, 102, 83, 112, 0, 0, 0, 252});
    assertArrayEquals(expected, result);

    cdo.reset();
    BytesCodec.writeBytesRaw(cdo, "fYfSp".getBytes());
    result = cdo.toBytes();
    expected = toBytes(new int[] {102, 89, 102, 83, 112});
    assertArrayEquals(expected, result);
  }

  @Test
  public void writeFloatTest() {
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
  public void readFloatTest() {
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
  public void negativeLongTest() {
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
    ExtendedDateTime time = new ExtendedDateTime(new DateTime(1999, 12, 12, 1, 1, 1, 999), 999);
    // Encode as UTC (loss timezone) and read it back as UTC
    ExtendedDateTime time1 =
        requireNonNull(DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time, utc), utc));
    assertTrue(equalExtendedDateTime(time, time1));

    // Parse String as -8 timezone, encode and read it back
    DateTimeFormatter formatter =
        DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSSSSSS").withZone(otherTz);
    ExtendedDateTime time2 =
        new ExtendedDateTime(DateTime.parse("2010-10-10 10:11:11:123456", formatter));
    ExtendedDateTime time3 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time2, otherTz), otherTz));
    assertTrue(equalExtendedDateTime(time2, time3));

    // when packedLong is 0, then null is returned
    ExtendedDateTime time4 = DateTimeCodec.fromPackedLong(0, otherTz);
    assertNull(time4);

    ExtendedDateTime time5 =
        new ExtendedDateTime(DateTime.parse("9999-12-31 23:59:59:123456", formatter));
    ExtendedDateTime time6 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time5, otherTz), otherTz));
    assertTrue(equalExtendedDateTime(time5, time6));

    ExtendedDateTime time7 =
        new ExtendedDateTime(DateTime.parse("1000-01-01 00:00:00:123456", formatter));
    ExtendedDateTime time8 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time7, otherTz), otherTz));
    assertTrue(equalExtendedDateTime(time7, time8));

    ExtendedDateTime time9 =
        new ExtendedDateTime(DateTime.parse("2017-01-05 23:59:59:5756010", formatter));
    ExtendedDateTime time10 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time9, otherTz), otherTz));
    assertTrue(equalExtendedDateTime(time9, time10));

    DateTimeFormatter formatter1 = DateTimeFormat.forPattern("yyyy-MM-dd");
    ExtendedDateTime date1 = new ExtendedDateTime(DateTime.parse("2099-10-30", formatter1));
    long time11 = DateTimeCodec.toPackedLong(date1, otherTz);
    ExtendedDateTime time12 = requireNonNull(DateTimeCodec.fromPackedLong(time11, otherTz));
    assertTrue(equalExtendedDateTime(time12, date1));

    Calendar cal = Calendar.getInstance(otherTz.toTimeZone());
    cal.set(Calendar.HOUR_OF_DAY, 0);
    cal.set(Calendar.MINUTE, 0);
    cal.set(Calendar.SECOND, 0);
    cal.set(Calendar.MILLISECOND, 0);
    long millis = cal.getTimeInMillis();
    ExtendedDateTime time14 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(DateCodec.toPackedLong(millis, otherTz), otherTz));
    assertEquals(millis, time14.getDateTime().getMillis());
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
    ExtendedDateTime time12 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(
                ((((2007L * 13 + 3) << 5L | 11) << 17L) | (2 << 12)) << 24L, dst));
    ExtendedDateTime time22 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(
                ((((2007L * 13 + 3) << 5L | 11) << 17L) | (1 << 12)) << 24L, dst));
    ExtendedDateTime time23 =
        requireNonNull(
            DateTimeCodec.fromPackedLong(
                ((((2007L * 13 + 3) << 5L | 11) << 17L) | (7 << 12)) << 24L, utc));
    DateTime time24 = time23.getDateTime().toDateTime(dst);
    // time11: 2007-03-11T03:00:00.000-04:00
    // time21: 2007-03-11T01:00:00.000-05:00
    // time12: 2007-03-11T03:00:00.000-04:00
    // time22: 2007-03-11T01:00:00.000-05:00
    // time23: 2007-03-11T07:00:00.000Z
    // time24: 2007-03-11T03:00:00.000-04:00
    assertEquals(time11.getMillis(), time12.getDateTime().getMillis());
    assertEquals(time21.getMillis(), time22.getDateTime().getMillis());
    assertEquals(time12.getDateTime().getMillis(), time23.getDateTime().getMillis());
    assertEquals(time23.getDateTime().getMillis(), time24.getMillis());
    assertEquals(
        time22.getDateTime().getMillis() + 60 * 60 * 1000, time23.getDateTime().getMillis());

    assertEquals(time12.getDateTime().toLocalDateTime().getHourOfDay(), 3);
    assertEquals(time22.getDateTime().toLocalDateTime().getHourOfDay(), 1);
    assertEquals(time23.getDateTime().toLocalDateTime().getHourOfDay(), 7);
    assertEquals(time24.toLocalDateTime().getHourOfDay(), 3);

    TimeZone.setDefault(defaultTimeZone);
    DateTimeZone.setDefault(defaultDateTimeZone);
  }

  @Test
  public void readNWriteDateTimeTest() {
    DateTimeZone otherTz = DateTimeZone.forOffsetHours(-8);
    ExtendedDateTime time =
        new ExtendedDateTime(new DateTime(2007, 3, 11, 2, 0, 0, 0, DateTimeZone.UTC));

    CodecDataOutput cdo = new CodecDataOutput();
    DateTimeCodec.writeDateTimeFully(cdo, time, otherTz);
    DateTimeCodec.writeDateTimeProto(cdo, time, otherTz);

    assertArrayEquals(
        new byte[] {
          (byte) 0x4,
          (byte) 0x19,
          (byte) 0x7b,
          (byte) 0x95,
          (byte) 0x20,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x19,
          (byte) 0x7b,
          (byte) 0x95,
          (byte) 0x20,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
        },
        cdo.toBytes());

    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(UINT_FLAG, cdi.readByte());
    ExtendedDateTime time2 = DateTimeCodec.readFromUInt(cdi, otherTz);
    assertEquals(time.getDateTime().getMillis(), time2.getDateTime().getMillis());
    time2 = DateTimeCodec.readFromUInt(cdi, otherTz);
    assertEquals(time.getDateTime().getMillis(), time2.getDateTime().getMillis());

    cdo.reset();
    IntegerCodec.writeULongFully(cdo, DateTimeCodec.toPackedLong(time, otherTz), false);
    cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(UVARINT_FLAG, cdi.readByte());
    time2 = DateTimeCodec.readFromUVarInt(cdi, otherTz);
    assertEquals(time.getDateTime().getMillis(), time2.getDateTime().getMillis());
  }

  @Test
  public void readNWriteDateTest() {
    DateTimeZone defaultDateTimeZone = DateTimeZone.getDefault();
    DateTimeZone testTz = DateTimeZone.forOffsetHours(-8);
    TimeZone defaultTimeZone = TimeZone.getDefault();
    TimeZone testTimeZone = TimeZone.getTimeZone("GMT-8");

    DateTimeZone.setDefault(testTz);
    TimeZone.setDefault(testTimeZone);

    Date time = new Date(new LocalDate(2007, 3, 11).toDateTimeAtStartOfDay(testTz).getMillis());

    CodecDataOutput cdo = new CodecDataOutput();
    DateCodec.writeDateFully(cdo, time, testTz);
    DateCodec.writeDateProto(cdo, time, testTz);

    assertArrayEquals(
        new byte[] {
          (byte) 0x4,
          (byte) 0x19,
          (byte) 0x7b,
          (byte) 0x96,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x19,
          (byte) 0x7b,
          (byte) 0x96,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
          (byte) 0x00,
        },
        cdo.toBytes());

    CodecDataInput cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(UINT_FLAG, cdi.readByte());
    LocalDate time2 = DateCodec.readFromUInt(cdi);
    assertEquals(time.getTime(), time2.toDate().getTime());
    time2 = DateCodec.readFromUInt(cdi);
    assertEquals(time.getTime(), time2.toDate().getTime());

    cdo.reset();
    IntegerCodec.writeULongFully(cdo, DateCodec.toPackedLong(time, testTz), false);
    cdi = new CodecDataInput(cdo.toBytes());
    assertEquals(UVARINT_FLAG, cdi.readByte());
    time2 = DateCodec.readFromUVarInt(cdi);
    assertEquals(time.getTime(), time2.toDate().getTime());

    DateTimeZone.setDefault(defaultDateTimeZone);
    TimeZone.setDefault(defaultTimeZone);
  }

  private boolean equalExtendedDateTime(ExtendedDateTime edt1, ExtendedDateTime edt2) {
    return edt1.getMicrosOfMillis() == edt2.getMicrosOfMillis()
        && edt1.getDateTime().getMillis() == edt2.getDateTime().getMillis();
  }
}
