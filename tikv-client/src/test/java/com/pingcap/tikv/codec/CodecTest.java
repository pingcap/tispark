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
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.codec.Codec.DateTimeCodec;
import com.pingcap.tikv.codec.Codec.DecimalCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.Codec.RealCodec;
import java.math.BigDecimal;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;

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
    DecimalCodec.writeDecimal(cdo, BigDecimal.valueOf(206.0));
    BigDecimal bigDec = DecimalCodec.readDecimal(new CodecDataInput(cdo.toBytes()));
    assertEquals(206.0, bigDec.doubleValue(), 0.0001);
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
      assertTrue(true);
    }
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
    DateTime time1 = DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time, utc), utc);
    assertEquals(time.getMillis(), time1.getMillis());

    // Parse String as -8 timezone, encode and read it back
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss:SSSSSSS").withZone(otherTz);
    DateTime time2 = DateTime.parse("2010-10-10 10:11:11:0000000", formatter);
    DateTime time3 = DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time2, otherTz), otherTz);
    assertEquals(time2.getMillis(), time3.getMillis());

    // when packedLong is 0, then null is returned
    DateTime time4 = DateTimeCodec.fromPackedLong(0, otherTz);
    assertNull(time4);

    DateTime time5 = DateTime.parse("9999-12-31 23:59:59:0000000", formatter);
    DateTime time6 = DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time5, otherTz), otherTz);
    assertEquals(time5.getMillis(), time6.getMillis());

    DateTime time7 = DateTime.parse("1000-01-01 00:00:00:0000000", formatter);
    DateTime time8 = DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time7, otherTz), otherTz);
    assertEquals(time7.getMillis(), time8.getMillis());

    DateTime time9 = DateTime.parse("2017-01-05 23:59:59:5756010", formatter);
    DateTime time10 = DateTimeCodec.fromPackedLong(DateTimeCodec.toPackedLong(time9, otherTz), otherTz);
    assertEquals(time9.getMillis(), time10.getMillis());

    DateTimeFormatter formatter1 = DateTimeFormat.forPattern("yyyy-MM-dd");
    DateTime date1 = DateTime.parse("2099-10-30", formatter1);
    long time11 = DateTimeCodec.toPackedLong(date1, otherTz);
    DateTime time12 = DateTimeCodec.fromPackedLong(time11, otherTz);
    assertEquals(time12.getMillis(), date1.getMillis());
  }
}