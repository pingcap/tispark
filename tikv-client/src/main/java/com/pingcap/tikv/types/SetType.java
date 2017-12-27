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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.codec.UnsupportedTypeException;
import com.pingcap.tikv.meta.TiColumnInfo;

public class SetType extends BytesType {
  private SetType(int tp) {
    super(tp);
  }

  protected SetType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  static SetType of(int tp) {
    return new SetType(tp);
  }

  public String simpleTypeName() { return "set"; }

  // Set is not supported yet
  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    throw new UnsupportedTypeException("Set type is not supported yet");
  }
}
