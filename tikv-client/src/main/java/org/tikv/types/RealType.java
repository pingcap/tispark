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

package org.tikv.types;

import com.pingcap.tidb.tipb.ExprType;
import org.tikv.codec.Codec;
import org.tikv.codec.Codec.DecimalCodec;
import org.tikv.codec.Codec.RealCodec;
import org.tikv.codec.CodecDataInput;
import org.tikv.codec.CodecDataOutput;
import org.tikv.exception.InvalidCodecFormatException;
import org.tikv.exception.TypeException;
import org.tikv.meta.TiColumnInfo;

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
  public Object getOriginDefaultValueNonNull(String value) {
    return Double.parseDouble(value);
  }
}
