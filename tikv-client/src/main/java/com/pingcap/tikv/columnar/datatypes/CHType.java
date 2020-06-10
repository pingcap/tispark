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

package com.pingcap.tikv.columnar.datatypes;

import static com.pingcap.tikv.util.MemoryUtil.allocateDirect;

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.columnar.TiBlockColumnVector;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.util.MemoryUtil;
import java.nio.ByteBuffer;

// TODO Support nullable data types.
// TODO Support nested, array and struct types.
public abstract class CHType {
  protected int length;
  protected boolean nullable = false;

  abstract String name();

  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  protected ByteBuffer decodeNullMap(CodecDataInput cdi, int size) {
    // read size * uint8 from cdi
    ByteBuffer buffer = allocateDirect(size);
    MemoryUtil.readFully(buffer, cdi, size);
    buffer.clear();
    return buffer;
  }

  public abstract DataType toDataType();

  protected int bufferSize(int size) {
    return size * length;
  }

  public TiBlockColumnVector decode(CodecDataInput cdi, int size) {
    if (length == -1) {
      throw new IllegalStateException("var type should have its own decode method");
    }

    if (size == 0) {
      return new TiBlockColumnVector(this);
    }
    if (isNullable()) {
      ByteBuffer nullMap = decodeNullMap(cdi, size);
      ByteBuffer buffer = allocateDirect(bufferSize(size));
      // read bytes from cdi to buffer(off-heap)
      MemoryUtil.readFully(buffer, cdi, bufferSize(size));
      buffer.clear();
      return new TiBlockColumnVector(this, nullMap, buffer, size, length);
    } else {
      ByteBuffer buffer = allocateDirect(bufferSize(size));
      MemoryUtil.readFully(buffer, cdi, bufferSize(size));
      buffer.clear();
      return new TiBlockColumnVector(this, buffer, size, length);
    }
  }
}
