package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateType;

public class CHTypeMyDate extends CHType {
  public CHTypeMyDate() {
    this.length = 8;
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
