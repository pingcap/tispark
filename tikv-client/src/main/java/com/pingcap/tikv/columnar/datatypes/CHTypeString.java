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

import static com.pingcap.tikv.util.MemoryUtil.EMPTY_BYTE_BUFFER_DIRECT;
import static com.pingcap.tikv.util.MemoryUtil.allocateDirect;

import com.google.common.base.Preconditions;
import com.pingcap.tikv.codec.Codec.IntegerCodec;
import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.columnar.TiBlockColumnVector;
import com.pingcap.tikv.types.DataType;
import com.pingcap.tikv.types.StringType;
import com.pingcap.tikv.util.MemoryUtil;
import java.nio.ByteBuffer;

public class CHTypeString extends CHType {
  // Use to prevent frequently reallocate the chars buffer.
  // ClickHouse does not pass a total length at the beginning, so sad...
  private static final ThreadLocal<ByteBuffer> initBuffer =
      ThreadLocal.withInitial(() -> allocateDirect(102400));

  public CHTypeString() {
    this.length = -1;
  }

  @Override
  public String name() {
    return "String";
  }

  @Override
  public DataType toDataType() {
    return StringType.TEXT;
  }

  @Override
  public TiBlockColumnVector decode(CodecDataInput cdi, int size) {
    if (size == 0) {
      return new TiBlockColumnVector(this);
    }

    ByteBuffer nullMap;
    if (isNullable()) {
      nullMap = decodeNullMap(cdi, size);
    } else {
      nullMap = EMPTY_BYTE_BUFFER_DIRECT;
    }

    ByteBuffer offsets = allocateDirect(size << 3);
    ByteBuffer initCharsBuf = initBuffer.get();
    AutoGrowByteBuffer autoGrowCharsBuf = new AutoGrowByteBuffer(initCharsBuf);

    int offset = 0;
    for (int i = 0; i < size; i++) {
      int valueSize = (int) IntegerCodec.readUVarLong(cdi);

      offset += valueSize + 1;
      offsets.putLong(offset);

      autoGrowCharsBuf.put(cdi, valueSize);
      autoGrowCharsBuf.putByte((byte) 0); // terminating zero byte
    }

    Preconditions.checkState(offset == autoGrowCharsBuf.dataSize());

    ByteBuffer chars = autoGrowCharsBuf.getByteBuffer();
    if (chars == initCharsBuf) {
      // Copy out.
      ByteBuffer newChars = allocateDirect(offset);
      MemoryUtil.copyMemory(MemoryUtil.getAddress(chars), MemoryUtil.getAddress(newChars), offset);
      chars = newChars;
    }

    return new TiBlockColumnVector(this, nullMap, offsets, chars, size);
  }
}
