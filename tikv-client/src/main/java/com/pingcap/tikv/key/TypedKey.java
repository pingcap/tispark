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

package com.pingcap.tikv.key;

import static java.util.Objects.requireNonNull;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.codec.KeyUtils;
import com.pingcap.tikv.exception.TypeException;
import com.pingcap.tikv.types.BytesType;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.IntegerType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.util.LogDesensitization;

public class TypedKey extends Key {
  private final DataType type;
  private final int prefixLength;

  public TypedKey(Object val, DataType type, int prefixLength) {
    super(encodeKey(val, type, prefixLength));
    this.type = type;
    this.prefixLength = prefixLength;
  }

  private TypedKey(byte[] val, DataType type) {
    super(val);
    this.type = type;
    this.prefixLength = DataType.UNSPECIFIED_LEN;
  }

  /**
   * Map a typed value into TypedKey, only encoding first prefixLength bytes
   *
   * <p>When prefixLength is DataType.UNSPECIFIED_LEN, encode full length of value
   *
   * @param val value
   * @param type type of value
   * @param prefixLength described above
   * @return an encoded TypedKey
   */
  public static TypedKey toTypedKey(Object val, DataType type, int prefixLength) {
    requireNonNull(type, "type is null");
    return new TypedKey(val, type, prefixLength);
  }

  public static TypedKey toTypedKey(Object val, DataType type) {
    return toTypedKey(val, type, DataType.UNSPECIFIED_LEN);
  }

  private static byte[] encodeKey(Object val, DataType type, int prefixLength) {
    CodecDataOutput cdo = new CodecDataOutput();
    type.encodeKey(cdo, val, prefixLength);
    return cdo.toBytes();
  }

  public DataType getType() {
    return type;
  }

  public Object getValue() {
    CodecDataInput cdi = new CodecDataInput(value);
    return type.decode(cdi);
  }

  @Override
  public TypedKey nextPrefix() {
    return toRawTypedKey(prefixNext(value), type);
  }

  private TypedKey toRawTypedKey(byte[] val, DataType type) {
    return new TypedKey(val, type);
  }

  /**
   * Next TypedKey be truncated with prefixLength
   *
   * @return next TypedKey with same prefix length
   */
  @Override
  public TypedKey next() {
    DataType tp = getType();
    Object val = getValue();
    if (tp instanceof StringType) {
      return toTypedKey(prefixNext(((String) val).getBytes()), type, prefixLength);
    } else if (tp instanceof BytesType) {
      return toTypedKey(prefixNext(((byte[]) val)), type, prefixLength);
    } else if (DataType.isLengthUnSpecified(prefixLength)) {
      if (tp instanceof IntegerType) {
        return toTypedKey(((long) val) + 1, type);
      } else {
        // use byte array type when next key is hard to identify
        return toRawTypedKey(prefixNext(value), type);
      }
    } else {
      throw new TypeException(
          "When prefix length is defined, type for TypedKey in next() function must be either String or Byte array. Actual: "
              + val.getClass().getName());
    }
  }

  @Override
  public String toString() {
    try {
      CodecDataInput cdi = new CodecDataInput(value);
      Object val = type.decode(cdi);
      if (val instanceof byte[]) {
        return LogDesensitization.hide(KeyUtils.formatBytes(value));
      }
      return val.toString();
    } catch (Exception e) {
      return "raw value:" + LogDesensitization.hide(KeyUtils.formatBytesUTF8(value));
    }
  }
}
