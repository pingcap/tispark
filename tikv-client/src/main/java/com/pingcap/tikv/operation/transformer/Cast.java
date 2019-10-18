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
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DecimalType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.RealType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.types.TypeSystem;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;

public class Cast extends NoOp {
  public Cast(DataType type) {
    super(type);
  }

  @Override
  public void set(Object value, Row row, int pos) {
    if (TypeSystem.getVersion() == 1) {
      castV1(value, row, pos);
    } else {
      castV0(value, row, pos);
    }
  }

  private void castV1(Object value, Row row, int pos) {
    if (value == null) {
      row.set(pos, targetDataType, null);
      return;
    }
    if (targetDataType instanceof IntegerType) {
      Object casted;

      String returnType = ((IntegerType) targetDataType).getV1ReturnJavaType();
      if ("Long".equals(returnType)) {
        casted = castToLong(value);
      } else if ("Integer".equals(returnType)) {
        casted = castToInteger(value);
      } else if ("BigDecimal".equals(returnType)) {
        casted = castToDecimal(value);
      } else if ("Boolean".equals(returnType)) {
        casted = castToBoolean(value);
      } else if ("Date".equals(returnType)) {
        casted = value;
      } else {
        casted = value;
      }

      row.set(pos, targetDataType, casted);
    } else {
      castV0(value, row, pos);
    }
  }

  private void castV0(Object value, Row row, int pos) {
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
    if (obj instanceof Double) {
      return (Double) obj;
    }

    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.doubleValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to double ");
  }

  private BigDecimal castToDecimal(Object obj) {
    if (obj instanceof BigDecimal) {
      return (BigDecimal) obj;
    }

    if (obj instanceof Number) {
      Number num = (Number) obj;
      return new BigDecimal(num.doubleValue());
    }
    throw new UnsupportedOperationException(
        "Cannot cast to BigDecimal: " + (obj == null ? "null" : obj.getClass().getSimpleName()));
  }

  private Boolean castToBoolean(Object obj) {
    if (obj instanceof Boolean) {
      return (Boolean) obj;
    }

    return castToLong(obj) != 0;
  }

  private Integer castToInteger(Object obj) {
    if (obj instanceof Integer) {
      return (Integer) obj;
    }

    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.intValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to integer ");
  }

  private Long castToLong(Object obj) {
    if (obj instanceof Long) {
      return (Long) obj;
    }

    if (obj instanceof Number) {
      Number num = (Number) obj;
      return num.longValue();
    }
    throw new UnsupportedOperationException("can not cast un-number to long ");
  }

  private String castToString(Object obj) {
    if (obj instanceof String) {
      return (String) obj;
    }

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
