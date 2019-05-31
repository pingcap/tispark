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
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SetType extends DataType {
  public static final SetType SET = new SetType(MySQLType.TypeSet);

  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeSet};

  private SetType(MySQLType tp) {
    super(tp);
  }

  private long[] setIndexValue = initSetIndexVal();
  private long[] setIndexInvertValue = initSetIndexInvertVal();

  private long[] initSetIndexInvertVal() {
    long[] tmpArr = new long[64];
    for (int i = 0; i < 64; i++) {
      // In TiDB code base, it is taking complement of original value.
      // setIndexInvertValue[i] = ^setIndexValue[i] is invalid in Java.
      tmpArr[i] = setIndexValue[i] ^ -1L;
    }
    return tmpArr;
  }

  private long[] initSetIndexVal() {
    long[] tmpArr = new long[64];
    for (int i = 0; i < 64; i++) {
      tmpArr[i] = 1L << i;
    }
    return tmpArr;
  }

  protected SetType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.UVARINT_FLAG) throw new TypeException("Invalid IntegerType flag: " + flag);
    long number = IntegerCodec.readUVarLong(cdi);
    List<String> items = new ArrayList<>();
    int length = this.getElems().size();
    for (int i = 0; i < length; i++) {
      // Long.MIN_VALUE is -9223372036854775808 with 1000...000 binary string.
      // setIndexValue[63] is also Long.MIN_VALUE, hence number & setIndexValue yields
      // Long.MIN_VALUE which is not 0(other cases will yield 0)
      long checker = number & setIndexValue[i];
      if (checker != 0) {
        items.add(this.getElems().get(i));
        number &= setIndexInvertValue[i];
      }
    }

    if (number != 0) {
      throw new TypeException(String.format("invalid number %d for Set %s", number, getElems()));
    }

    return items.stream().collect(Collectors.joining(","));
  }

  @Override
  public Object convertToTiDBType(Object value)
      throws TypeConvertNotSupportException, ConvertDataOverflowException {
    return new TypeConvertNotSupportException(
        value.getClass().getName(), this.getClass().getName());
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
  public ExprType getProtoExprType() {
    return ExprType.MysqlSet;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return value;
  }
}
