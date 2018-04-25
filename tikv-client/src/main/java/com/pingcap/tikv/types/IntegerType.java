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
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

import java.math.BigDecimal;
import java.math.BigInteger;

public class IntegerType extends DataType {
  public static final IntegerType TINYINT = new IntegerType(MySQLType.TypeTiny);
  public static final IntegerType SMALLINT = new IntegerType(MySQLType.TypeShort);
  public static final IntegerType MEDIUMINT = new IntegerType(MySQLType.TypeInt24);
  public static final IntegerType INT = new IntegerType(MySQLType.TypeLong);
  public static final IntegerType BIGINT = new IntegerType(MySQLType.TypeLonglong);
  public static final IntegerType BOOLEAN = TINYINT;

  public static final MySQLType[] subTypes = new MySQLType[] {
      MySQLType.TypeTiny, MySQLType.TypeShort, MySQLType.TypeInt24,
      MySQLType.TypeLong, MySQLType.TypeLonglong
  };

  protected IntegerType(MySQLType tp) {
    super(tp);
  }

  private static final BigDecimal UNSIGNED_OFFSET = new BigDecimal(BigInteger.ONE.shiftLeft(64));

  private BigDecimal unsignedValueOf(long x) {
    if (x < 0) {
      return BigDecimal.valueOf(x).add(UNSIGNED_OFFSET);
    } else {
      return BigDecimal.valueOf(x);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    long ret;
    switch (flag) {
      case Codec.UVARINT_FLAG:
        ret = IntegerCodec.readUVarLong(cdi);
        if (tp == MySQLType.TypeLonglong) {
          return unsignedValueOf(ret);
        }
        return ret;
      case Codec.UINT_FLAG:
        ret = IntegerCodec.readULong(cdi);
        if (tp == MySQLType.TypeLonglong) {
          return unsignedValueOf(ret);
        }
        return ret;
      case Codec.VARINT_FLAG:
        return IntegerCodec.readVarLong(cdi);
      case Codec.INT_FLAG:
        return IntegerCodec.readLong(cdi);
      default:
        throw new TypeException("Invalid IntegerType flag: " + flag);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    if (isUnsigned()) {
      IntegerCodec.writeULongFully(cdo, longVal, true);
    } else {
      IntegerCodec.writeLongFully(cdo, longVal, true);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    if (isUnsigned()) {
      IntegerCodec.writeULongFully(cdo, longVal, false);
    } else {
      IntegerCodec.writeLongFully(cdo, longVal, false);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    if (isUnsigned()) {
      IntegerCodec.writeULong(cdo, longVal);
    } else {
      IntegerCodec.writeLong(cdo, longVal);
    }
  }

  @Override
  public ExprType getProtoExprType() {
    return isUnsigned() ?  ExprType.Uint64 : ExprType.Int64;
  }

  public boolean isUnsigned() {
    return (flag & UnsignedFlag) > 0;
  }
  /**
   * {@inheritDoc}
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return Integer.parseInt(value);
  }

  protected IntegerType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

}
