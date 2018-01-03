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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import org.junit.Test;

public class IntegerTypeTest {
  @Test
  public void readNWriteLongTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerType.writeLongFull(cdo, 9999L, true);
    IntegerType.writeLongFull(cdo, -2333L, false);
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
    long value = IntegerType.readLongFully(cdi);
    assertEquals(9999L, value);
    value = IntegerType.readLongFully(cdi);
    assertEquals(-2333L, value);

    byte[] wrongData = new byte[] {(byte) 0x8, (byte) 0xb9};
    cdi = new CodecDataInput(wrongData);
    try {
      IntegerType.readLongFully(cdi);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void readNWriteUnsignedLongTest() throws Exception {
    CodecDataOutput cdo = new CodecDataOutput();
    IntegerType.writeULongFull(cdo, 0xffffffffffffffffL, true);
    IntegerType.writeULongFull(cdo, Long.MIN_VALUE, false);
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
    long value = IntegerType.readULongFully(cdi);

    assertEquals(0xffffffffffffffffL, value);
    value = IntegerType.readULongFully(cdi);
    assertEquals(Long.MIN_VALUE, value);

    byte[] wrongData =
        new byte[] {
          (byte) 0x9, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80, (byte) 0x80, (byte) 0x80,
          (byte) 0x80, (byte) 0x80
        };
    cdi = new CodecDataInput(wrongData);
    try {
      IntegerType.readULongFully(cdi);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }
}
