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

package org.tikv.operation.transformer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import org.tikv.row.Row;
import org.tikv.types.*;

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
    } else if (targetDataType instanceof StringType) {
      casted = castToString(value);
    } else if (targetDataType instanceof BytesType) {
      casted = castToBinary(value);
    } else if (targetDataType instanceof DecimalType) {
      casted = castToDecimal(value);
    } else if (targetDataType instanceof RealType) {
      casted = castToDouble(value);
    } else {
      casted = value;
    }
    row.set(pos, targetDataType, casted);
  }

  private Double castToDouble(Object obj) {
    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.doubleValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to double ");
  }

  private BigDecimal castToDecimal(Object obj) {
    if (obj instanceof Number) {
      Number num = (Number) obj;
      return new BigDecimal(num.doubleValue());
    }
    throw new UnsupportedOperationException(
        "Cannot cast to BigDecimal: " + (obj == null ? "null" : obj.getClass().getSimpleName()));
  }

  private Long castToLong(Object obj) {
    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.longValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to long ");
  }

  private String castToString(Object obj) {
    String result;
    if (obj instanceof byte[]) {
      result = new String((byte[]) obj, StandardCharsets.UTF_8);
    } else if (obj instanceof char[]) {
      result = new String((char[]) obj);
    } else {
      result = String.valueOf(obj);
    }
    return result;
  }

  private byte[] castToBinary(Object obj) {
    if (obj instanceof byte[]) {
      return (byte[]) obj;
    } else {
      return obj.toString().getBytes();
    }
  }
}
