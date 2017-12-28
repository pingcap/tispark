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

import static org.junit.Assert.*;

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import org.junit.Test;

public class RealTypeTest {
  @Test
  public void writeFloatTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    RealType.writeDouble(cdo, 0.00);
    double u = RealType.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(0.00, u, 0);

    cdo.reset();
    RealType.writeDouble(cdo, Double.MAX_VALUE);
    u = RealType.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(Double.MAX_VALUE, u, 0);

    cdo.reset();
    RealType.writeDouble(cdo, Double.MIN_VALUE);
    u = RealType.readDouble(new CodecDataInput(cdo.toBytes()));
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
    double u = RealType.readDouble(cdi);
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
    u = RealType.readDouble(cdi);
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
    u = RealType.readDouble(cdi);
    assertEquals(-99.199, u, 0.0001);
  }

  @Test
  public void negativeLongTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerType.writeULong(cdo, UnsignedLong.valueOf("13831004815617530266").longValue());
    double u = RealType.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(1.1, u, 0.001);

    cdo.reset();
    IntegerType.writeULong(cdo, UnsignedLong.valueOf("13835508415244900762").longValue());
    u = RealType.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(2.2, u, 0.001);

    cdo.reset();
    IntegerType.writeULong(cdo, UnsignedLong.valueOf("13837985394932580352").longValue());
    u = RealType.readDouble(new CodecDataInput(cdo.toBytes()));
    assertEquals(3.3, u, 0.001);
  }
}
