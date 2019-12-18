package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.StringType;

public class CHTypeFixedString extends CHType {
  private int length;

  public CHTypeFixedString(int length) {
    assert length > 0;
    this.length = length;
  }

  @Override
  public String name() {
    return "FixedString(" + length + ")";
  }

  @Override
  public DataType toDataType() {
    return StringType.TEXT;
  }
}
