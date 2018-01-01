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

/**
 * Type TimeType
 * refers to mysql time
 */
public class TimeType extends DateTimeType {
  private TimeType(int tp) {
    super(tp);
  }

  protected TimeType(TiColumnInfo.InternalTypeHolder holder) {
    super(holder);
  }

  static TimeType of(int tp) {
    return new TimeType(tp);
  }

  public String simpleTypeName() { return "time"; }

  // Time is not supported yet
  @Override
  public Object decodeNotNull(int flag, CodecDataInput cdi) {
    throw new UnsupportedTypeException("Time type is not supported yet");
  }
}
