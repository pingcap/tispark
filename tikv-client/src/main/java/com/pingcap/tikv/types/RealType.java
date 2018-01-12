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
import com.pingcap.tikv.codec.Codec.RealCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.InvalidCodecFormatException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class RealType extends DataType {
  public static final RealType DOUBLE = new RealType(MySQLType.TypeDouble);
  public static final RealType FLOAT = new RealType(MySQLType.TypeFloat);
  public static final RealType REAL = DOUBLE;

  public static final MySQLType[] subTypes = new MySQLType[] {
      MySQLType.TypeDouble,
      MySQLType.TypeFloat
  };

  private RealType(MySQLType tp) {
    super(tp);
  }

  RealType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    // check flag first and then read.
    if (flag != Codec.FLOATING_FLAG) {
      throw new InvalidCodecFormatException("Invalid Flag type for float type: " + flag);
    }
    return RealCodec.readDouble(cdi);
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

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Double.parseDouble(value);
  }
}
