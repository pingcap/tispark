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
import com.pingcap.tikv.exception.TypeException;
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

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.UVARINT_FLAG) throw new TypeException("Invalid IntegerType flag: " + flag);
    return this.getElems().get((int) IntegerCodec.readUVarLong(cdi) - 1);
  }

  /** {@inheritDoc} Enum is encoded as unsigned int64 with its 0-based value. */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    IntegerCodec.writeULongFully(cdo, Converter.convertToLong(value), true);
  }

  /** {@inheritDoc} Enum is encoded as unsigned int64 with its 0-based value. */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    IntegerCodec.writeULongFully(cdo, Converter.convertToLong(value), false);
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    IntegerCodec.writeULong(cdo, Converter.convertToLong(value));
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlEnum;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return value;
  }
}
