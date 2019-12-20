package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;

public class CHTypeMyDateTime extends CHType {

  public static final CHTypeMyDateTime instance = new CHTypeMyDateTime();

  private CHTypeMyDateTime() {
    this.length = 3;
  }

  @Override
  public String name() {
    return "MyDateTime";
  }

  @Override
  public DataType toDataType() {
    return DateTimeType.DATETIME;
  }
}
