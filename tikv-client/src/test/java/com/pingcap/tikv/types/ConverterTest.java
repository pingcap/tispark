package com.pingcap.tikv.types;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConverterTest {

  @Test
  public void convertToLongTest() {}

  @Test
  public void convertToDoubleTest() {}

  @Test
  public void convertToStringTest() {}

  @Test
  public void convertToBytesTest() {}

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
