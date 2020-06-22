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
import com.pingcap.tikv.codec.Codec.SetCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.exception.UnsupportedTypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class SetType extends DataType {
  public static final SetType SET = new SetType(MySQLType.TypeSet);

  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeSet};

  private SetType(MySQLType tp) {
    super(tp);
  }

  protected SetType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    long number;
    switch (flag) {
      case Codec.UVARINT_FLAG:
        number = IntegerCodec.readUVarLong(cdi);
        break;
      case Codec.UINT_FLAG:
        number = IntegerCodec.readULong(cdi);
        break;
      default:
        throw new TypeException("Invalid IntegerType flag: " + flag);
    }

    return SetCodec.readSetFromLong(number, this.getElems());
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    return new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Set type cannot be pushed down.");
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Set type cannot be pushed down.");
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Set type cannot be pushed down.");
  }

  @Override
  public String getName() {
    return "SET";
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlSet;
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
