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
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.UnsupportedTypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

/**
 * TODO: Support Year Type
 * YearType class is set now only to indicate this type exists,
 * so that we could throw UnsupportedTypeException when encountered.
 * Its logic is not yet implemented.
 *
 * Since year type acts differently in Spark and MySQL --
 *  for instance, in MySQL, year is an unsigned integer(2017),
 *  whereas in Spark, year is treated as Date(2017-01-01).
 * -- it is not decided which logic should inherit.
 *
 * Year is encoded as unsigned int64.
 */
public class YearType extends DataType {
  public static final YearType YEAR = new YearType(MySQLType.TypeYear);

  public static final MySQLType[] subTypes = new MySQLType[] {
      MySQLType.TypeYear
  };

  private YearType(MySQLType tp) {
    super(tp);
  }

  protected YearType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    throw new UnsupportedTypeException("Year type not supported");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Year type not supported");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Year type not supported");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedTypeException("Year type not supported");
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.Int64;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getOriginDefaultValueNonNull(String value) {
    return value;
  }
}
