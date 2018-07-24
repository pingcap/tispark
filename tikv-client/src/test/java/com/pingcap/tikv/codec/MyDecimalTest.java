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

package com.pingcap.tikv.codec;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class MyDecimalTest {
  @Test
  public void toIntTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("-9223372036854775807", "-9223372036854775807"));
    tests.add(new MyDecimalTestStruct("-9223372036854775808", "-9223372036854775808"));
    tests.add(new MyDecimalTestStruct("9223372036854775808", "9223372036854775807"));
    tests.add(new MyDecimalTestStruct("-9223372036854775809", "-9223372036854775808"));
    tests.add(new MyDecimalTestStruct("18446744073709551615", "9223372036854775807"));

    for(MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      String result = dec.toInt() + "";
      assertEquals(t.out, result);
    }
  }

  @Test
  public void fromStringTest() throws Exception {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("1111111111111111111111111.111111111111111111111111111111",
        "1111111111111111111111111.111111111111111111111111111111", 65, 30));
    tests.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("12345.", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("123.45", "123.45", 5, 2));
    tests.add(new MyDecimalTestStruct("-123.45", "-123.45", 5, 2));
    tests.add(new MyDecimalTestStruct(".00012345000098765", "0.00012345000098765", 17, 17));
    tests.add(new MyDecimalTestStruct(".12345000098765", "0.12345000098765", 14, 14));
    tests.add(
        new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 21, 21));
    tests.add(
        new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 2, 21));
     tests.add(
        new MyDecimalTestStruct("-.000000012345000098765", "-0.000000012345000098765", 21, 2));
    tests.add(new MyDecimalTestStruct("0000000.001", "0.001", 3, 3));
    tests.add(new MyDecimalTestStruct("1234500009876.5", "1234500009876.5", 14, 1));
    for(MyDecimalTestStruct t : tests) {
           MyDecimal dec = new MyDecimal();
          dec.fromString(t.in);
          assertEquals(t.out, dec.toString());
    }
  }

  @Test
  public void readWordTest() throws Exception {
    assertEquals(MyDecimal.readWord(new int[]{250}, 1, 0), -6);
    assertEquals(MyDecimal.readWord(new int[]{50}, 1, 0), 50);

    assertEquals(MyDecimal.readWord(new int[]{250, 250}, 2, 0), -1286);
    assertEquals(MyDecimal.readWord(new int[]{50, 50}, 2, 0), 12850);

    assertEquals(MyDecimal.readWord(new int[]{250, 250, 250}, 3, 0), -328966);
    assertEquals(MyDecimal.readWord(new int[]{50, 50, 50}, 3, 0), 3289650);

    assertEquals(MyDecimal.readWord(new int[]{250, 250, 250, 250}, 4, 0), -84215046);
    assertEquals(MyDecimal.readWord(new int[]{50, 50, 50, 50}, 4, 0), 842150450);
  }

  @Test
  public void toBinFromBinTest() throws Exception {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    String decValStr = "11111111111111111111111111111111111.111111111111111111111111111111";
    tests.add(new MyDecimalTestStruct(decValStr, decValStr, 65, 30));
    tests.add(new MyDecimalTestStruct("12345000098765", "12345000098765", 14, 0));
    tests.add(new MyDecimalTestStruct("-10.55", "-10.55", 4, 2));
    tests.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("-12345", "-12345", 5, 0));
    tests.add(new MyDecimalTestStruct("0000000.001", "0.001", 3, 3));
    tests.add(new MyDecimalTestStruct("0.00012345000098765", "0.00012345000098765", 17, 17));
    tests.add(new MyDecimalTestStruct("-0.00012345000098765", "-0.00012345000098765", 17, 17));
    for (MyDecimalTestStruct a : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(a.in);
      assertEquals(a.out, dec.toString());
      int[] bin = dec.toBin(a.precision, a.frac);
      dec.clear();
      dec.fromBin(a.precision, a.frac, bin);
      assertEquals(a.out, dec.toString());
    }
  }

  @Test
  public void toBinTest() throws Exception {
    MyDecimal dec = new MyDecimal();
    dec.fromDecimal(-1234567890.1234);
    int[] data = dec.toBin(dec.precision(), dec.frac());
    int[] expected =
        new int[] {
          0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D,
        };
    assertArrayEquals(expected, data);
  }

  // MyDecimalTestStruct is only used for simplifing testing.
  private class MyDecimalTestStruct {
    String in;
    String out;
    int precision;
    int frac;


    MyDecimalTestStruct(String in, String out) {
      this.in = in;
      this.out = out;
    }
    MyDecimalTestStruct(String in, String out, int precision, int frac) {
      this.in = in;
      this.out = out;
      this.precision = precision;
      this.frac = frac;
    }
  }
}
