package com.pingcap.tikv.columnar.datatypes;

import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DecimalType;

public class CHTypeDecimal extends CHType {
  public int precision, scale;

  public CHTypeDecimal(int precision, int scale) {
    this.precision = precision;
    this.scale = scale;
    if (precision <= 9) {
      length = 4;
    } else if (precision <= 18) {
      length = 8;
    } else if (precision <= 38) {
      length = 16;
    } else {
      length = 48;
    }
  }

  protected int bufferSize(int size) {
    return size * length;
  }

  @Override
  public String name() {
    return "Decimal(" + precision + ", " + scale + ")";
  }

  @Override
  public DataType toDataType() {
    return new DecimalType(precision, scale);
  }
}
