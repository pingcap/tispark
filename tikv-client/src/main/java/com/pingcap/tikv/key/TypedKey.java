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
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.DataType.EncodeType;

public class TypedKey extends Key {
  private final DataType type;

  private TypedKey(Object val, DataType type) {
    super(encode(val, type));
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
    requireNonNull(type, "type is null");
    return new TypedKey(val, type);
  }

  private static byte[] encode(Object val, DataType type) {
    CodecDataOutput cdo = new CodecDataOutput();
    type.encode(cdo, EncodeType.KEY, val);
    return cdo.toBytes();
  }

  @Override
  public String toString() {
    CodecDataInput cdi = new CodecDataInput(value);
    Object val = type.decode(cdi);
    return String.format("%s", val);
  }
}
