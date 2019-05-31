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
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertDataOverflowException;
import com.pingcap.tikv.exception.TypeConvertNotSupportException;
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
  public Object convertToTiDBType(Object value)
      throws TypeConvertNotSupportException, ConvertDataOverflowException {
    return convertToMysqlEnum(value);
  }

  private Integer convertToMysqlEnum(Object value) throws TypeConvertNotSupportException {
    Integer result;

    if (value instanceof String) {
      result = parseEnumName((String) value);
    } else {
      Long l = Converter.safeConvertToUnsigned(value, this.unsignedUpperBound());
      result = parseEnumValue(l.intValue());
    }
    return result;
  }

  private Integer parseEnumName(String name) {
    int i = 0;
    while (i < this.getElems().size()) {
      if (this.getElems().get(i).equals(name)) {
        return i + 1;
      }
      i = i + 1;
    }

    // name doesn't exist, maybe an integer?
    int result = Integer.parseInt(name);
    return parseEnumValue(result);
  }

  private Integer parseEnumValue(Integer number) throws ConvertDataOverflowException {
    if (number == 0) {
      throw ConvertDataOverflowException.newLowerBound(number, 0);
    }

    if (number > this.getElems().size()) {
      throw ConvertDataOverflowException.newUpperBound(number, this.getElems().size());
    }

    return number;
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.UVARINT_FLAG)
      throw new TypeException("Invalid EnumType(IntegerType) flag: " + flag);
    int idx = (int) IntegerCodec.readUVarLong(cdi) - 1;
    if (idx < 0 || idx >= this.getElems().size())
      throw new TypeException("Index is out of range, better " + "take a look at tidb side.");
    return this.getElems().get(idx);
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
  public ExprType getProtoExprType() {
    return ExprType.MysqlEnum;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return value;
  }
}
