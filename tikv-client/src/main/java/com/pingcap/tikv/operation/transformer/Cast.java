/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package com.pingcap.tikv.operation.transformer;

import com.pingcap.tikv.row.Row;
import com.pingcap.tikv.types.*;
import java.math.BigDecimal;

public class Cast extends NoOp {
  public Cast(DataType type) {
    super(type);
  }

  @Override
  public void set(Object value, Row row, int pos) {
    Object casted;
    if (value == null) {
      row.set(pos, targetDataType, null);
      return;
    }
    if (targetDataType instanceof IntegerType) {
      casted = castToLong(value);
    } else if (targetDataType instanceof BytesType) {
      casted = castToString(value);
    } else if (targetDataType instanceof DecimalType) {
      casted = castToDecimal(value);
    } else if (targetDataType instanceof RealType) {
      casted = castToDouble(value);
    } else {
      casted = value;
    }
    row.set(pos, targetDataType, casted);
  }

  public Double castToDouble(Object obj) {
    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.doubleValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to double ");
  }

  public BigDecimal castToDecimal(Object obj) {
    if (obj instanceof Number) {
      Number num = (Number) obj;
      return new BigDecimal(num.doubleValue());
    } else if (obj instanceof BigDecimal) {
      return (BigDecimal) obj;
    }
    throw new UnsupportedOperationException(
        "can not cast to BigDecimal: " + obj == null ? "null" : obj.getClass().getSimpleName());
  }

  public Long castToLong(Object obj) {
    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.longValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to long ");
  }

  public String castToString(Object obj) {
    return obj.toString();
  }
}
