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

package com.pingcap.tikv.types;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.DecimalCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertDataOverflowException;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import com.pingcap.tikv.exception.TypeConvertNotSupportException;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.math.BigDecimal;

public class DecimalType extends DataType {
  public static final DecimalType DECIMAL = new DecimalType(MySQLType.TypeNewDecimal);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeNewDecimal};

  private DecimalType(MySQLType tp) {
    super(tp);
  }

  DecimalType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.DECIMAL_FLAG) {
      throw new InvalidCodecFormatException("Invalid Flag type for decimal type: " + flag);
    }
    return DecimalCodec.readDecimal(cdi);
  }

  @Override
  public Object convertToTiDBType(Object value)
      throws TypeConvertNotSupportException, ConvertDataOverflowException {
    return convertToMysqlDecimal(value);
  }

  private java.math.BigDecimal convertToMysqlDecimal(Object value)
      throws TypeConvertNotSupportException {
    java.math.BigDecimal result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = BigDecimal.ONE;
      } else {
        result = BigDecimal.ZERO;
      }
    } else if (value instanceof Byte) {
      result = java.math.BigDecimal.valueOf(((Byte) value).longValue());
    } else if (value instanceof Short) {
      result = java.math.BigDecimal.valueOf(((Short) value).longValue());
    } else if (value instanceof Integer) {
      result = java.math.BigDecimal.valueOf(((Integer) value).longValue());
    } else if (value instanceof Long) {
      result = java.math.BigDecimal.valueOf((Long) value);
    } else if (value instanceof Float) {
      result = java.math.BigDecimal.valueOf(((Float) value).doubleValue());
    } else if (value instanceof Double) {
      result = java.math.BigDecimal.valueOf((Double) value);
    } else if (value instanceof String) {
      result = new java.math.BigDecimal((String) value);
    } else if (value instanceof java.math.BigDecimal) {
      result = (java.math.BigDecimal) value;
    } else {
      throw new TypeConvertNotSupportException(
          value.getClass().getName(), this.getClass().getName());
    }
    return result;
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    DecimalCodec.writeDecimalFully(cdo, val);
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    DecimalCodec.writeDecimalFully(cdo, val);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    DecimalCodec.writeDecimal(cdo, val);
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlDecimal;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return new BigDecimal(value);
  }
}
