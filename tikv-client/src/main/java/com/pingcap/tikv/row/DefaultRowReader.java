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

package com.pingcap.tikv.row;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.types.DataType;

public class DefaultRowReader implements RowReader {
  private final CodecDataInput cdi;

  DefaultRowReader(CodecDataInput cdi) {
    this.cdi = cdi;
  }

  public static DefaultRowReader create(CodecDataInput cdi) {
    return new DefaultRowReader(cdi);
  }

  public Row readRow(DataType[] dataTypes) {
    int length = dataTypes.length;
    Row row = ObjectRowImpl.create(length);
    for (int i = 0; i < length; i++) {
      DataType type = dataTypes[i];
      if (type.isNextNull(cdi)) {
        cdi.readUnsignedByte();
        row.setNull(i);
      } else {
        row.set(i, type, type.decode(cdi));
      }
    }
    return row;
  }
}
