/*
 * Copyright 2020 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

  @Override
  public String name() {
    return "Decimal(" + precision + ", " + scale + ")";
  }

  @Override
  public DataType toDataType() {
    return new DecimalType(precision, scale);
  }
}
