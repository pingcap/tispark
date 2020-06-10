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

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tidb.tipb.ExprType;
import com.pingcap.tikv.codec.Codec;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.meta.Collation;
import com.pingcap.tikv.meta.TiColumnInfo;
import java.math.BigDecimal;

public class IntegerType extends DataType {
  public static final IntegerType TINYINT = new IntegerType(MySQLType.TypeTiny);
  public static final IntegerType SMALLINT = new IntegerType(MySQLType.TypeShort);
  public static final IntegerType MEDIUMINT = new IntegerType(MySQLType.TypeInt24);
  public static final IntegerType INT = new IntegerType(MySQLType.TypeLong);
  public static final IntegerType BIGINT = new IntegerType(MySQLType.TypeLonglong);
  public static final IntegerType BOOLEAN = TINYINT;
  public static final IntegerType YEAR = new IntegerType(MySQLType.TypeYear);

  public static final IntegerType ROW_ID_TYPE =
      new IntegerType(MySQLType.TypeLonglong, PriKeyFlag, 20, 0);

  public static final MySQLType[] subTypes =
      new MySQLType[] {
        MySQLType.TypeTiny,
        MySQLType.TypeShort,
        MySQLType.TypeInt24,
        MySQLType.TypeLong,
        MySQLType.TypeLonglong,
        MySQLType.TypeYear
      };

  protected IntegerType(MySQLType type, int flag, int len, int decimal) {
    super(type, flag, len, decimal, "", Collation.DEF_COLLATION_CODE);
  }

  protected IntegerType(MySQLType tp) {
    super(tp);
  }

  protected IntegerType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  private static BigDecimal unsignedValueOf(long x) {
    return new BigDecimal(UnsignedLong.fromLongBits(x).bigIntegerValue());
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    // TODO: support write to YEAR
    if (this.getType() == MySQLType.TypeYear) {
      throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
    }

    Long result;
    if (this.isUnsigned()) {
      result = Converter.safeConvertToUnsigned(value, this.unsignedUpperBound());
    } else {
      result =
          Converter.safeConvertToSigned(value, this.signedLowerBound(), this.signedUpperBound());
    }

    return result;
  }

  public boolean isSameCatalog(DataType other) {
    return other instanceof IntegerType;
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    long ret;
    switch (flag) {
      case Codec.UVARINT_FLAG:
        ret = IntegerCodec.readUVarLong(cdi);
        break;
      case Codec.UINT_FLAG:
        ret = IntegerCodec.readULong(cdi);
        break;
      case Codec.VARINT_FLAG:
        ret = IntegerCodec.readVarLong(cdi);
        break;
      case Codec.INT_FLAG:
        ret = IntegerCodec.readLong(cdi);
        break;
      default:
        throw new TypeException("Invalid IntegerType flag: " + flag);
    }
    if (isUnsignedLong()) {
      return unsignedValueOf(ret);
    }
    return ret;
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    if (isUnsigned()) {
      IntegerCodec.writeULongFully(cdo, longVal, true);
    } else {
      IntegerCodec.writeLongFully(cdo, longVal, true);
    }
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    long longVal = Converter.convertToLong(value);
    if (isUnsigned()) {
      IntegerCodec.writeULongFully(cdo, longVal, false);
    } else {
      IntegerCodec.writeLongFully(cdo, longVal, false);
    }
  }

  /** {@inheritDoc} */
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
  public String getName() {
    if (isUnsigned()) {
      return "UNSIGNED LONG";
    }
    return "LONG";
  }

  @Override
  public ExprType getProtoExprType() {
    return isUnsigned() ? ExprType.Uint64 : ExprType.Int64;
  }

  public boolean isUnsignedLong() {
    return tp == MySQLType.TypeLonglong && isUnsigned();
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    return Long.parseLong(value);
  }
}
