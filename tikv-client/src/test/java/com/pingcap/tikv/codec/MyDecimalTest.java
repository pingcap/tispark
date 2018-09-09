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

import java.util.ArrayList;
import java.util.List;

import org.apache.ivy.util.StringUtils;
import org.junit.Test;

import static org.junit.Assert.*;

public class MyDecimalTest {
  @Test
  public void toLongTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("-9223372036854775807", "-9223372036854775807"));
    tests.add(new MyDecimalTestStruct("-9223372036854775808", "-9223372036854775808"));
    tests.add(new MyDecimalTestStruct("9223372036854775808", "9223372036854775807"));
    tests.add(new MyDecimalTestStruct("-9223372036854775809", "-9223372036854775808"));
    tests.add(new MyDecimalTestStruct("18446744073709551615", "9223372036854775807"));
    tests.add(new MyDecimalTestStruct("151202", "151202"));
    tests.add(new MyDecimalTestStruct("-151202", "-151202"));
    tests.add(new MyDecimalTestStruct("1512.02", "1512"));
    tests.add(new MyDecimalTestStruct("-1512.02", "-1512"));
    tests.add(new MyDecimalTestStruct("1512.200", "1512"));
    tests.add(new MyDecimalTestStruct("-1512.200", "-1512"));
    tests.add(new MyDecimalTestStruct("1512.00000000000000200", "1512"));
    tests.add(new MyDecimalTestStruct("1512.00000000000000", "1512"));

    // from tidb
    tests.add(new MyDecimalTestStruct("18446744073709551615", "9223372036854775807"));
    tests.add(new MyDecimalTestStruct("-1", "-1"));
    tests.add(new MyDecimalTestStruct("1", "1"));
    tests.add(new MyDecimalTestStruct("-1.23", "-1"));

    for(MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      String result = dec.toLong() + "";
      assertEquals(t.out, result);
    }
  }

  @Test
  public void maxDecimalTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct(1, 1, "0.9"));
    tests.add(new MyDecimalTestStruct(1, 0, "9"));
    tests.add(new MyDecimalTestStruct(2, 1, "9.9"));
    tests.add(new MyDecimalTestStruct(4, 2, "99.99"));
    tests.add(new MyDecimalTestStruct(6, 3, "999.999"));
    tests.add(new MyDecimalTestStruct(8, 4, "9999.9999"));
    tests.add(new MyDecimalTestStruct(10, 5, "99999.99999"));
    tests.add(new MyDecimalTestStruct(12, 6, "999999.999999"));
    tests.add(new MyDecimalTestStruct(14, 7, "9999999.9999999"));
    tests.add(new MyDecimalTestStruct(16, 8, "99999999.99999999"));
    tests.add(new MyDecimalTestStruct(18, 9, "999999999.999999999"));
    tests.add(new MyDecimalTestStruct(20, 10, "9999999999.9999999999"));
    tests.add(new MyDecimalTestStruct(20, 20, "0.99999999999999999999"));
    tests.add(new MyDecimalTestStruct(20, 0, "99999999999999999999"));
    tests.add(new MyDecimalTestStruct(40, 20, "99999999999999999999.99999999999999999999"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.maxDecimal(t.precision, t.frac);
      String str = dec.toString();
      assertEquals(t.out, str);
    }
  }

  @Test
  public void fromDoubleTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct(12345, "12345.0"));
    tests.add(new MyDecimalTestStruct(123.45, "123.45"));
    tests.add(new MyDecimalTestStruct(-123.45, "-123.45"));
    tests.add(new MyDecimalTestStruct(0.00012345000098765, "0.00012345000098765"));
    tests.add(new MyDecimalTestStruct(1234500009876.5, "1234500009876.5"));

    for(MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromDecimal(t.din);
      assertEquals(t.out, dec.toString());
    }
  }

  @Test
  public void toDoubleTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct(12345, "12345.0"));
    tests.add(new MyDecimalTestStruct(123.45, "123.45"));
    tests.add(new MyDecimalTestStruct(-123.45, "-123.45"));
    tests.add(new MyDecimalTestStruct(0.00012345000098765, "0.00012345000098765"));
    tests.add(new MyDecimalTestStruct(1234500009876.5, "1234500009876.5"));
    tests.add(new MyDecimalTestStruct(-0.00012345000098765, "-0.00012345000098765"));
    tests.add(new MyDecimalTestStruct(-1234500009876.5, "-1234500009876.5"));

    for(MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.out);
      assertEquals(beautify(t.din), beautify(dec.toDouble()), 1e-8);
    }
  }

  @Test
  public void fromStringTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    // TODO: deal with UINT64
    // tests.add(new MyDecimalTestStruct("1e18446744073709551620", "0"));
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
    tests.add(new MyDecimalTestStruct("+1234500009876.5", "1234500009876.5", 14, 1));
    // test overflow
    tests.add(new MyDecimalTestStruct(StringUtils.repeat("111111111", 10), StringUtils.repeat("111111111", 9)));
    // test truncate
    tests.add(new MyDecimalTestStruct("1." + StringUtils.repeat("111111111", 10), "1." + StringUtils.repeat("111111111", 8)));
    tests.add(new MyDecimalTestStruct("11." + StringUtils.repeat("111111111", 12), "11." + StringUtils.repeat("111111111", 8)));
    tests.add(new MyDecimalTestStruct("123e", "123"));

    // for science notation
    tests.add(new MyDecimalTestStruct("123E5", "12300000"));
    tests.add(new MyDecimalTestStruct("123E-2", "1.23"));
    tests.add(new MyDecimalTestStruct("1e001", "10"));
    tests.add(new MyDecimalTestStruct("1e00", "1"));
    tests.add(new MyDecimalTestStruct("1e -1", "0.1"));
    tests.add(new MyDecimalTestStruct("1e1073741823", "999999999999999999999999999999999999999999999999999999999999999999999999999999999"));
    tests.add(new MyDecimalTestStruct("-1e1073741823", "-999999999999999999999999999999999999999999999999999999999999999999999999999999999"));
    tests.add(new MyDecimalTestStruct("1e", "1"));
    tests.add(new MyDecimalTestStruct("1eabc", "1"));
    tests.add(new MyDecimalTestStruct("1e 1dddd ", "10"));
    tests.add(new MyDecimalTestStruct("1e - 1", "1"));


    for(MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      assertEquals(t.out, dec.toString());
    }

    List<MyDecimalTestStruct> wrongTests = new ArrayList<>();
    wrongTests.add(new MyDecimalTestStruct("", "0"));
    wrongTests.add(new MyDecimalTestStruct("  ", "0"));
    wrongTests.add(new MyDecimalTestStruct(".", "0"));
    wrongTests.add(new MyDecimalTestStruct("error", "0"));

    for (MyDecimalTestStruct wt : wrongTests) {
      try {
        MyDecimal dec = new MyDecimal();
        dec.fromString(wt.in);
        fail();
      } catch (Exception e) {
        assertTrue(true);
      }
    }

    MyDecimal.wordBufLen = 1;

    MyDecimal dec = new MyDecimal();
    dec.fromString("123450000098765");
    String result = dec.toString();
    assertEquals("98765", result);

    dec = new MyDecimal();
    dec.fromString("123450.000098765");
    result = dec.toString();
    assertEquals("123450", result);

    MyDecimal.wordBufLen = MyDecimal.maxWordBufLen;
  }

  @Test
  public void toStringTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("123.123", "123.123"));
    tests.add(new MyDecimalTestStruct("123.1230", "123.1230"));
    tests.add(new MyDecimalTestStruct("00123.123", "123.123"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      String result = dec.toString();
      assertEquals(t.out, result);
    }
  }

  @Test
  public void shiftTest() {
    MyDecimal.wordBufLen = MyDecimal.maxWordBufLen;
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("123.123", 1, "1231.23"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 1, "1234571891.23123456789"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 8, "12345718912312345.6789"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 9, "123457189123123456.789"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 10, "1234571891231234567.89"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 17, "12345718912312345678900000"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 18, "123457189123123456789000000"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 19, "1234571891231234567890000000"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 26, "12345718912312345678900000000000000"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 27, "123457189123123456789000000000000000"));
    tests.add(new MyDecimalTestStruct("123457189.123123456789000", 28, "1234571891231234567890000000000000000"));
    tests.add(new MyDecimalTestStruct("000000000000000000000000123457189.123123456789000", 26, "12345718912312345678900000000000000"));
    tests.add(new MyDecimalTestStruct("00000000123457189.123123456789000", 27, "123457189123123456789000000000000000"));
    tests.add(new MyDecimalTestStruct("00000000000000000123457189.123123456789000", 28, "1234571891231234567890000000000000000"));
    tests.add(new MyDecimalTestStruct("123", 1, "1230"));
    tests.add(new MyDecimalTestStruct("123", 10, "1230000000000"));
    tests.add(new MyDecimalTestStruct(".123", 1, "1.23"));
    tests.add(new MyDecimalTestStruct(".123", 10, "1230000000"));
    tests.add(new MyDecimalTestStruct(".123", 14, "12300000000000"));
    tests.add(new MyDecimalTestStruct("000.000", 1000, "0"));
    tests.add(new MyDecimalTestStruct("000.", 1000, "0"));
    tests.add(new MyDecimalTestStruct(".000", 1000, "0"));
    tests.add(new MyDecimalTestStruct("123.123", -1, "12.3123"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -1, "12398765432.1123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -2, "1239876543.21123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -3, "123987654.321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -8, "1239.87654321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -9, "123.987654321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -10, "12.3987654321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -11, "1.23987654321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -12, "0.123987654321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -13, "0.0123987654321123456789"));
    tests.add(new MyDecimalTestStruct("123987654321.123456789000", -14, "0.00123987654321123456789"));
    tests.add(new MyDecimalTestStruct("00000087654321.123456789000", -14, "0.00000087654321123456789"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal.MyDecimalError err = dec.shiftDecimal(t.scale);
      assertEquals(MyDecimal.MyDecimalError.noError, err);
      String result = dec.toString();
      assertEquals(t.out, result);
    }

    List<MyDecimalTestStruct> wrongTests = new ArrayList<>();
    wrongTests.add(new MyDecimalTestStruct("1", 1000, "1", MyDecimal.MyDecimalError.errOverflow));
    for (MyDecimalTestStruct t : wrongTests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal.MyDecimalError err = dec.shiftDecimal(t.scale);
      assertEquals(t.error, err);
      String result = dec.toString();
      assertEquals(t.out, result);
    }

    MyDecimal.wordBufLen = 2;
    tests.clear();
    tests.add(new MyDecimalTestStruct("123.123", -2, "1.23123"));
    tests.add(new MyDecimalTestStruct("123.123", -3, "0.123123"));
    tests.add(new MyDecimalTestStruct("123.123", -6, "0.000123123"));
    tests.add(new MyDecimalTestStruct("123.123", -7, "0.0000123123"));
    tests.add(new MyDecimalTestStruct("123.123", -15, "0.000000000000123123"));
    tests.add(new MyDecimalTestStruct(".000000000123", -6, "0.000000000000000123"));
    tests.add(new MyDecimalTestStruct(".000000000123", -1, "0.0000000000123"));
    tests.add(new MyDecimalTestStruct(".000000000123", 1, "0.00000000123"));
    tests.add(new MyDecimalTestStruct(".000000000123", 8, "0.0123"));
    tests.add(new MyDecimalTestStruct(".000000000123", 9, "0.123"));
    tests.add(new MyDecimalTestStruct(".000000000123", 10, "1.23"));
    tests.add(new MyDecimalTestStruct(".000000000123", 17, "12300000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 18, "123000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 19, "1230000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 20, "12300000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 21, "123000000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 22, "1230000000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 23, "12300000000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 24, "123000000000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 25, "1230000000000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 26, "12300000000000000"));
    tests.add(new MyDecimalTestStruct(".000000000123", 27, "123000000000000000"));
    tests.add(new MyDecimalTestStruct("123456789.987654321", -9, "0.123456789987654321"));
    tests.add(new MyDecimalTestStruct("123456789.987654321", 9, "123456789987654321"));
    tests.add(new MyDecimalTestStruct("123456789.987654321", 0, "123456789.987654321"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal.MyDecimalError err = dec.shiftDecimal(t.scale);
      assertEquals(MyDecimal.MyDecimalError.noError, err);
      String result = dec.toString();
      assertEquals(t.out, result);
    }

    wrongTests.clear();
    wrongTests.add(new MyDecimalTestStruct("123.123", -16, "0.000000000000012312", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123.123", -17, "0.000000000000001231", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123.123", -18, "0.000000000000000123", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123.123", -19, "0.000000000000000012", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123.123", -20, "0.000000000000000001", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123.123", -21, "0", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct(".000000000123", -7, "0.000000000000000012", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct(".000000000123", -8, "0.000000000000000001", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct(".000000000123", -9, "0", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct(".000000000123", 28, "0.000000000123", MyDecimal.MyDecimalError.errOverflow));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", 10, "123456789.987654321", MyDecimal.MyDecimalError.errOverflow));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -1, "12345678.998765432", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -2, "1234567.899876543", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -8, "1.234567900", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -10, "0.012345678998765432", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -17, "0.000000001234567900", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -18, "0.000000000123456790", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -19, "0.000000000012345679", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -26, "0.000000000000000001", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", -27, "0", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", 1, "1234567900", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", 2, "12345678999", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", 4, "1234567899877", MyDecimal.MyDecimalError.errTruncated));
    wrongTests.add(new MyDecimalTestStruct("123456789.987654321", 8, "12345678998765432", MyDecimal.MyDecimalError.errTruncated));

    for (MyDecimalTestStruct t : wrongTests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal.MyDecimalError err = dec.shiftDecimal(t.scale);
      assertEquals(t.error, err);
      String result = dec.toString();
      assertEquals(t.out, result);
    }

    MyDecimal.wordBufLen = MyDecimal.maxWordBufLen;
  }


  @Test
  public void roundWithHalfEvenTest() throws Exception {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("123456789.987654321", 1, "123456790.0"));
    tests.add(new MyDecimalTestStruct("15.1", 0, "15"));
    tests.add(new MyDecimalTestStruct("15.5", 0, "16"));
    tests.add(new MyDecimalTestStruct("15.9", 0, "16"));
    tests.add(new MyDecimalTestStruct("-15.1", 0, "-15"));
    tests.add(new MyDecimalTestStruct("-15.5", 0, "-16"));
    tests.add(new MyDecimalTestStruct("-15.9", 0, "-16"));
    tests.add(new MyDecimalTestStruct("15.1", 1, "15.1"));
    tests.add(new MyDecimalTestStruct("-15.1", 1, "-15.1"));
    tests.add(new MyDecimalTestStruct("15.17", 1, "15.2"));
    tests.add(new MyDecimalTestStruct("15.4", -1, "20"));
    tests.add(new MyDecimalTestStruct("-15.4", -1, "-20"));
    tests.add(new MyDecimalTestStruct("5.4", -1, "10"));
    tests.add(new MyDecimalTestStruct(".999", 0, "1"));
    tests.add(new MyDecimalTestStruct("999999999", -9, "1000000000"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal rounded = new MyDecimal();
      MyDecimal.MyDecimalError err = dec.round(rounded, t.scale, MyDecimal.RoundMode.modeHalfEven);
      assertEquals(MyDecimal.MyDecimalError.noError, err);
      String result = rounded.toString();
      assertEquals(t.out, result);
    }
  }

  @Test
  public void roundWithTruncateTest() throws Exception {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("123456789.987654321", 1, "123456789.9"));
    tests.add(new MyDecimalTestStruct("15.1", 0, "15"));
    tests.add(new MyDecimalTestStruct("15.5", 0, "15"));
    tests.add(new MyDecimalTestStruct("15.9", 0, "15"));
    tests.add(new MyDecimalTestStruct("-15.1", 0, "-15"));
    tests.add(new MyDecimalTestStruct("-15.5", 0, "-15"));
    tests.add(new MyDecimalTestStruct("-15.9", 0, "-15"));
    tests.add(new MyDecimalTestStruct("15.1", 1, "15.1"));
    tests.add(new MyDecimalTestStruct("-15.1", 1, "-15.1"));
    tests.add(new MyDecimalTestStruct("15.17", 1, "15.1"));
    tests.add(new MyDecimalTestStruct("15.4", -1, "10"));
    tests.add(new MyDecimalTestStruct("-15.4", -1, "-10"));
    tests.add(new MyDecimalTestStruct("5.4", -1, "0"));
    tests.add(new MyDecimalTestStruct(".999", 0, "0"));
    tests.add(new MyDecimalTestStruct("999999999", -9, "0"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal rounded = new MyDecimal();
      MyDecimal.MyDecimalError err = dec.round(rounded, t.scale, MyDecimal.RoundMode.modeTruncate);
      assertEquals(MyDecimal.MyDecimalError.noError, err);
      String result = rounded.toString();
      assertEquals(t.out, result);
    }
  }

  @Test
  public void roundWithCeilingTest() throws Exception {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct("123456789.987654321", 1, "123456790.0"));
    tests.add(new MyDecimalTestStruct("15.1", 0, "16"));
    tests.add(new MyDecimalTestStruct("15.5", 0, "16"));
    tests.add(new MyDecimalTestStruct("15.9", 0, "16"));
    //TODO:fix me
    tests.add(new MyDecimalTestStruct("-15.1", 0, "-16"));
    tests.add(new MyDecimalTestStruct("-15.5", 0, "-16"));
    tests.add(new MyDecimalTestStruct("-15.9", 0, "-16"));
    tests.add(new MyDecimalTestStruct("15.1", 1, "15.1"));
    tests.add(new MyDecimalTestStruct("-15.1", 1, "-15.1"));
    tests.add(new MyDecimalTestStruct("15.17", 1, "15.2"));
    tests.add(new MyDecimalTestStruct("15.4", -1, "20"));
    tests.add(new MyDecimalTestStruct("-15.4", -1, "-20"));
    tests.add(new MyDecimalTestStruct("5.4", -1, "10"));
    tests.add(new MyDecimalTestStruct(".999", 0, "1"));
    tests.add(new MyDecimalTestStruct("999999999", -9, "1000000000"));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(t.in);
      MyDecimal rounded = new MyDecimal();
      MyDecimal.MyDecimalError error = dec.round(rounded, t.scale, MyDecimal.RoundMode.modeCeiling);
      assertEquals(MyDecimal.MyDecimalError.noError, error);
      String result = rounded.toString();
      assertEquals(t.out, result);
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
    tests.add(new MyDecimalTestStruct("00000000001234567890.1234", "1234567890.1234", 14, 4));
    tests.add(new MyDecimalTestStruct("1234567890.1234", "1234567890.12340000000000", 24, 14));

    // copy from tidb tests
    tests.add(new MyDecimalTestStruct("-10.55", "-10.55", 4, 2));
    tests.add(new MyDecimalTestStruct("0.0123456789012345678912345", "0.0123456789012345678912345", 30, 25));
    tests.add(new MyDecimalTestStruct("12345", "12345", 5, 0));
    tests.add(new MyDecimalTestStruct("12345", "12345.000", 10, 3));
    tests.add(new MyDecimalTestStruct("123.45", "123.450", 10, 3));
    tests.add(new MyDecimalTestStruct("-123.45", "-123.4500000000", 20, 10));
    tests.add(new MyDecimalTestStruct(".00012345000098765", "0.00012345000098", 15, 14));
    tests.add(new MyDecimalTestStruct(".00012345000098765", "0.00012345000098765000", 22, 20));
    tests.add(new MyDecimalTestStruct(".12345000098765", "0.12345000098765000000", 30, 20));
    tests.add(new MyDecimalTestStruct("-.000000012345000098765", "-0.00000001234500009876", 30, 20));
    tests.add(new MyDecimalTestStruct("1234500009876.5", "1234500009876.50000", 30, 5));
    tests.add(new MyDecimalTestStruct("111111111.11", "11111111.11", 10, 2));
    tests.add(new MyDecimalTestStruct("000000000.01", "0.010", 7, 3));
    tests.add(new MyDecimalTestStruct("123.4", "123.40", 10, 2));
    tests.add(new MyDecimalTestStruct("1000", "0", 3, 0));

    for (MyDecimalTestStruct a : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromString(a.in);
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

    dec = new MyDecimal();
    dec.fromString("1234567890.1234");
    data = dec.toBin(dec.precision() - 2, dec.frac());
    expected =
        new int[] {
            0x82, 0x0F, 0x76, 0xD2, 0x04, 0xD2
        };
    assertArrayEquals(expected, data);

    dec = new MyDecimal();
    dec.fromString("1234567890.1234");
    data = dec.toBin(dec.precision() + 1, dec.frac() + 1);
    expected =
        new int[] {
            0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x00, 0x30, 0x34
        };
    assertArrayEquals(expected, data);

    dec = new MyDecimal();
    dec.fromString("1234567890");
    data = dec.toBin(dec.precision() + 1, dec.frac() + 1);
    expected =
        new int[] {
            0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x00
        };
    assertArrayEquals(expected, data);

    dec = new MyDecimal();
    dec.fromString("1234567890.1234");
    data = dec.toBin(dec.precision() + 10, dec.frac() + 10);
    expected =
        new int[] {
            0x81, 0x0D, 0xFB, 0x38, 0xD2, 0x7, 0x5a, 0xef, 0x40, 0x0, 0x0, 0x0
        };
    assertArrayEquals(expected, data);

    dec = new MyDecimal();
    dec.fromString("234567890.1234");
    data = dec.toBin(dec.precision() - 1, dec.frac());
    expected =
        new int[] {
            0x82, 0x0F, 0x76, 0xD2, 0x04, 0xD2
        };
    assertArrayEquals(expected, data);

    dec = new MyDecimal();
    dec.fromString("1234567890.1234");
    try {
      dec.toBin(dec.precision(), -1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }

    dec = new MyDecimal();
    dec.fromString("1234567890.1234");
    try {
      dec.toBin(100, -1);
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

  @Test
  public void fromBinTest() {
    List<MyDecimalTestStruct> tests = new ArrayList<>();
    tests.add(new MyDecimalTestStruct(new int[] {
        0x7E, 0xF2, 0x04, 0xC7, 0x2D, 0xFB, 0x2D
    }, "-1234567890.1234", 14, 4));
    // test overflow
    tests.add(new MyDecimalTestStruct(new int[] {
        0x86, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
    }, StringUtils.repeat("1", 81), 81, 0));

    // test truncate
    tests.add(new MyDecimalTestStruct(new int[] {
        0x86, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
        0x06, 0x9f, 0x6b, 0xc7,
    }, "0." + StringUtils.repeat("1", 81), 81, 81));

    for (MyDecimalTestStruct t : tests) {
      MyDecimal dec = new MyDecimal();
      dec.fromBin(t.precision, t.frac, t.bin);
      assertEquals(t.out, dec.toString());
    }

    List<MyDecimalTestStruct> wrongTests = new ArrayList<>();
    wrongTests.add(new MyDecimalTestStruct(new int[] {}, "0", 0, 0));
    for (MyDecimalTestStruct wt : wrongTests) {
      try {
        MyDecimal dec = new MyDecimal();
        dec.fromBin(wt.precision, wt.frac, wt.bin);
        fail();
      } catch (Exception e) {
        assertTrue(true);
      }
    }
  }

  // MyDecimalTestStruct is only used for simplifing testing.
  private class MyDecimalTestStruct {
    String in;
    String out;
    int[] bin;
    int precision;
    int frac;
    double din;
    int scale;
    MyDecimal.MyDecimalError error;


    MyDecimalTestStruct(String in, String out) {
      this.in = in;
      this.out = out;
    }
    MyDecimalTestStruct(double din, String out) {
      this.din = din;
      this.out = out;
    }
    MyDecimalTestStruct(String in, int scale, String out) {
      this.in = in;
      this.out = out;
      this.scale = scale;
    }
    MyDecimalTestStruct(String in, int scale, String out, MyDecimal.MyDecimalError error) {
      this.in = in;
      this.out = out;
      this.scale = scale;
      this.error = error;
    }
    MyDecimalTestStruct(int[] bin, String out, int precision, int frac) {
      this.bin = bin;
      this.out = out;
      this.precision = precision;
      this.frac = frac;
    }
    MyDecimalTestStruct(int precision, int frac, String out) {
      this.out = out;
      this.precision = precision;
      this.frac = frac;
    }
    MyDecimalTestStruct(String in, String out, int precision, int frac) {
      this.in = in;
      this.out = out;
      this.precision = precision;
      this.frac = frac;
    }
  }

  private double beautify(double in) {
    while (Math.abs(in) >= 10) {
      in /= 10;
    }
    return in;
  }
}
