package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateTimeType;

public class CHTypeMyDateTime extends CHType {
  public CHTypeMyDateTime() {
    this.length = 8 << 3;
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
