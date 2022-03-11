/*
 *
 * Copyright 2019 PingCAP, Inc.
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
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * UninitializedType is created to deal with MySQLType being 0. In TiDB, when type is 0, it
 * indicates the type is not initialized and will not be applied during calculation process.
 */
public class UninitializedType extends DataType {
  public static final UninitializedType DECIMAL = new UninitializedType(MySQLType.TypeDecimal);
  public static final UninitializedType NULL = new UninitializedType(MySQLType.TypeNull);
  public static final MySQLType[] subTypes =
      new MySQLType[] {MySQLType.TypeDecimal, MySQLType.TypeNull};

  private UninitializedType(MySQLType tp) {
    super(tp);
  }

  UninitializedType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  public String getName() {
    return "NULL";
  }

  @Override
  public ExprType getProtoExprType() {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }

  @Override
  public boolean isPushDownSupported() {
    throw new UnsupportedOperationException(
        "UninitializedType cannot be applied in calculation process.");
  }
}
