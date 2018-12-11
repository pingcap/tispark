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
import java.math.BigDecimal;
import org.tikv.codec.Codec;
import org.tikv.codec.Codec.DecimalCodec;
import org.tikv.codec.CodecDataInput;
import org.tikv.codec.CodecDataOutput;
import org.tikv.exception.InvalidCodecFormatException;
import org.tikv.meta.TiColumnInfo;

public class DecimalType extends DataType {
  public static final DecimalType DECIMAL = new DecimalType(MySQLType.TypeNewDecimal);
  public static final MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeNewDecimal};

  private DecimalType(MySQLType tp) {
    super(tp);
  }

  DecimalType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /** {@inheritDoc} */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.DECIMAL_FLAG) {
      throw new InvalidCodecFormatException("Invalid Flag type for decimal type: " + flag);
    }
    return DecimalCodec.readDecimal(cdi);
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    DecimalCodec.writeDecimalFully(cdo, val);
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    DecimalCodec.writeDecimalFully(cdo, val);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    BigDecimal val = Converter.convertToBigDecimal(value);
    DecimalCodec.writeDecimal(cdo, val);
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlDecimal;
  }

  /** {@inheritDoc} */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return new BigDecimal(value);
  }
}
