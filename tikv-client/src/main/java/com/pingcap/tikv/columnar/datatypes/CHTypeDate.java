package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DateType;

public class CHTypeDate extends CHType {
  public CHTypeDate() {
    this.length = 1;
  }

  @Override
  public String name() {
    return "Date";
  }

  @Override
  public DataType toDataType() {
    return DateType.DATE;
  }
}
