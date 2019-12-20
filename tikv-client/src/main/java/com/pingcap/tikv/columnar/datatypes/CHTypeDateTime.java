package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;

public class CHTypeDateTime extends CHType {
  public static final CHTypeDateTime instance = new CHTypeDateTime();

  private CHTypeDateTime() {
    this.length = 2;
  }

  @Override
  public String name() {
    return "DateTime";
  }

  @Override
  public DataType toDataType() {
    return DateTimeType.DATETIME;
  }
}
