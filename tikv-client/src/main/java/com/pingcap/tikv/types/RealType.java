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
import com.pingcap.tikv.codec.Codec.RealCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class RealType extends DataType {
  public static final RealType DOUBLE = new RealType(MySQLType.TypeDouble);
  public static final RealType FLOAT = new RealType(MySQLType.TypeFloat);
  public static final RealType REAL = DOUBLE;

  public static final MySQLType[] subTypes =
      new MySQLType[] {MySQLType.TypeDouble, MySQLType.TypeFloat};

  private RealType(MySQLType tp) {
    super(tp);
  }

  RealType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag == Codec.DECIMAL_FLAG) {
      return DecimalCodec.readDecimal(cdi).doubleValue();
    } else if (flag == Codec.FLOATING_FLAG) {
      return RealCodec.readDouble(cdi);
    }
    throw new InvalidCodecFormatException("Invalid Flag type for float type: " + flag);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToReal(value);
  }

  private Object convertToReal(Object value) throws ConvertNotSupportException {
    Double result;
    if (value instanceof Boolean) {
      if ((Boolean) value) {
        result = 1d;
      } else {
        result = 0d;
      }
    } else if (value instanceof Byte) {
      result = ((Byte) value).doubleValue();
    } else if (value instanceof Short) {
      result = ((Short) value).doubleValue();
    } else if (value instanceof Integer) {
      result = ((Integer) value).doubleValue();
    } else if (value instanceof Long) {
      result = ((Long) value).doubleValue();
    } else if (value instanceof Float) {
      result = ((Float) value).doubleValue();
    } else if (value instanceof Double) {
      result = (Double) value;
    } else if (value instanceof String) {
      result = Converter.stringToDouble((String) value);
    } else {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }

    if (this.getType() == MySQLType.TypeFloat) {
      return result.floatValue();
    } else {
      return result;
    }
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    double val = Converter.convertToDouble(value);
    RealCodec.writeDoubleFully(cdo, val);
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    double val = Converter.convertToDouble(value);
    RealCodec.writeDoubleFully(cdo, val);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    double val = Converter.convertToDouble(value);
    RealCodec.writeDouble(cdo, val);
  }

  @Override
  public String getName() {
    if (tp == MySQLType.TypeDouble) {
      return "DOUBLE";
    }
    return "FLOAT";
  }

  @Override
  public ExprType getProtoExprType() {
    if (tp == MySQLType.TypeDouble) {
      return ExprType.Float64;
    } else if (tp == MySQLType.TypeFloat) {
      return ExprType.Float32;
    }
    throw new TypeException("Unknown Type encoding proto " + tp);
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return Double.parseDouble(value);
  }
}
