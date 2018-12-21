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
      tmpArr[i] ^= 1 << i;
    }
    return tmpArr;
  }

  private long[] initSetIndexVal() {
    long[] tmpArr = new long[64];
    for (int i = 0; i < 64; i++) {
      tmpArr[i] = 1 << i;
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
    int number = (int) IntegerCodec.readUVarLong(cdi);
    List<String> items = new ArrayList<>();
    for (int i = 0; i < this.getElems().size(); i++) {
      if ((number & setIndexValue[i]) > 0) {
        items.add(this.getElems().get(i));
      }
    }

    if (number == 0) {
      throw new TypeException(String.format("invalid number %d for Set %s", number, getElems()));
    }

    return items.stream().collect(Collectors.joining(","));
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    IntegerCodec.writeULongFully(cdo, Converter.convertToLong(value), true);
  }

  /** {@inheritDoc} */
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
    return ExprType.MysqlSet;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return value;
  }
}
