/*
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
 */

package com.pingcap.tikv.types;

import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.EnumCodec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.exception.UnsupportedTypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class EnumType extends DataType {
  public static final EnumType ENUM = new EnumType(MySQLType.TypeEnum);

  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeEnum};

  private EnumType(MySQLType tp) {
    super(tp);
  }

  protected EnumType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return convertToMysqlEnum(value);
  }

  private Integer convertToMysqlEnum(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    Integer result;

    if (value instanceof String) {
      result = EnumCodec.parseEnumName((String) value, this.getElems());
    } else {
      Long l = Converter.safeConvertToUnsigned(value, this.unsignedUpperBound());
      result = EnumCodec.parseEnumValue(l.intValue(), this.getElems());
    }
    return result;
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    int idx;
    switch (flag) {
        // encodeKey write UINT_FLAG
      case Codec.UINT_FLAG:
        idx = (int) IntegerCodec.readULong(cdi) - 1;
        break;
        // encodeValue write UVARINT_FLAG
      case Codec.UVARINT_FLAG:
        idx = (int) IntegerCodec.readUVarLong(cdi) - 1;
        break;
      default:
        throw new TypeException("Invalid EnumType(IntegerType) flag: " + flag);
    }
    return EnumCodec.readEnumFromIndex(idx, this.getElems());
  }

  /** {@inheritDoc} Enum is encoded as unsigned int64 with its 0-based value. */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    IntegerCodec.writeULongFully(cdo, longVal, true);
  }

  /** {@inheritDoc} Enum is encoded as unsigned int64 with its 0-based value. */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    IntegerCodec.writeULongFully(cdo, longVal, false);
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Enum type cannot be pushed down.");
  }

  @Override
  public String getName() {
    return "ENUM";
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlEnum;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return value;
  }

  @Override
  public boolean isPushDownSupported() {
    return false;
  }
}
