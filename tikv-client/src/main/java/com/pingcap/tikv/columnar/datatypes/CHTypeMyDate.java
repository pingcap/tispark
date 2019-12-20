package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateType;

public class CHTypeMyDate extends CHType {
  public static final CHTypeMyDate instance = new CHTypeMyDate();

  private CHTypeMyDate() {
    this.length = 3;
  }

  @Override
  public String name() {
    return "MyDate";
  }

  @Override
  public DataType toDataType() {
    return DateType.DATE;
  }
}
