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

package org.tikv.types;

import com.pingcap.tidb.tipb.ExprType;
import org.tikv.codec.CodecDataInput;
import org.tikv.codec.CodecDataOutput;
import org.tikv.exception.UnsupportedTypeException;
import org.tikv.meta.TiColumnInfo;

/**
 * TODO: Support Enum Type EnumType class is set now only to indicate this type exists, so that we
 * could throw UnsupportedTypeException when encountered. Its logic is not yet implemented.
 */
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
    throw new UnsupportedTypeException("Enum type not supported");
  }

  /** {@inheritDoc} Enum is encoded as unsigned int64 with its 0-based value. */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Enum type not supported");
  }

  /** {@inheritDoc} Enum is encoded as unsigned int64 with its 0-based value. */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Enum type not supported");
  }

  /** {@inheritDoc} */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Enum type not supported");
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
