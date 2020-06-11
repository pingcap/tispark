/*
 * Copyright 2020 PingCAP, Inc.
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
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.exception.ConvertNotSupportException;
import com.pingcap.tikv.exception.ConvertOverflowException;
import com.pingcap.tikv.exception.InvalidCodecFormatException;
import com.pingcap.tikv.meta.TiColumnInfo;
import com.pingcap.tikv.util.JsonUtils;

public class JsonType extends DataType {

  public static final JsonType JSON = new JsonType(MySQLType.TypeJSON);

  public static MySQLType[] subTypes = new MySQLType[] {MySQLType.TypeJSON};

  protected JsonType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  public JsonType(MySQLType type) {
    super(type);
  }

  public JsonType(MySQLType type, int flag, int len, int decimal, String charset, int collation) {
    super(type, flag, len, decimal, charset, collation);
  }

  @Override
  protected Object decodeNotNull(int flag, CodecDataInput cdi) {
    if (flag != Codec.JSON_FLAG) {
      throw new InvalidCodecFormatException(
          "Invalid Flag type for " + getClass().getSimpleName() + ": " + flag);
    }
    return JsonUtils.parseJson(cdi).toString();
  }

  @Override
  protected Object doConvertToTiDBType(Object value)
      throws ConvertNotSupportException, ConvertOverflowException {
    throw new ConvertNotSupportException(value.getClass().getName(), this.getClass().getName());
  }

  @Override
  protected void encodeKey(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException("JsonType.encodeKey|value=" + value);
  }

  @Override
  protected void encodeValue(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException("JsonType.encodeValue|value=" + value);
  }

  @Override
  protected void encodeProto(CodecDataOutput cdo, Object value) {
    throw new UnsupportedOperationException("JsonType.encodeProto|value=" + value);
  }

  @Override
  public String getName() {
    return "JSON";
  }

  @Override
  public ExprType getProtoExprType() {
    return ExprType.MysqlJson;
  }

  @Override
  public Object getOriginDefaultValueNonNull(String value, long version) {
    throw new AssertionError("json can't have a default value");
  }

  @Override
  public boolean isPushDownSupported() {
    return false;
  }
}
