package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;

public class CHTypeDateTime extends CHType {
  public CHTypeDateTime() {
    this.length = 4;
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
