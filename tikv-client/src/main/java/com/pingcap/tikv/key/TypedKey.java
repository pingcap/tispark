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


import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.CodecDataOutput;
import com.pingcap.tikv.types.DataType;

import static java.util.Objects.requireNonNull;

public class TypedKey extends Key {
  private final DataType type;

  public TypedKey(Object val, DataType type, int prefixLength) {
    super(encodeKey(val, type, prefixLength));
    this.type = type;
  }

  public DataType getType() {
    return type;
  }

  public Object getValue() {
    CodecDataInput cdi = new CodecDataInput(value);
    return type.decode(cdi);
  }

  public static TypedKey toTypedKey(Object val, DataType type) {
    return toTypedKey(val, type, DataType.UNSPECIFIED_LEN);
  }

  /**
   * Map a typed value into TypedKey, only encoding first prefixLength bytes
   * When prefixLength is DataType.UNSPECIFIED_LEN, encode full length of value
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

  private static byte[] encodeKey(Object val, DataType type, int prefixLength) {
    CodecDataOutput cdo = new CodecDataOutput();
    type.encodeKey(cdo, val, type, prefixLength);
    return cdo.toBytes();
  }

  @Override
  public String toString() {
    CodecDataInput cdi = new CodecDataInput(value);
    Object val = type.decode(cdi);
    return String.format("%s", val);
  }
}
