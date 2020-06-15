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

import com.pingcap.tikv.codec.CodecDataInput;
import com.pingcap.tikv.util.MemoryUtil;
import java.nio.ByteBuffer;

public class AutoGrowByteBuffer {
  private final ByteBuffer initBuf;
  private ByteBuffer buf;

  public AutoGrowByteBuffer(ByteBuffer initBuf) {
    initBuf.clear();
    this.initBuf = initBuf;
    this.buf = initBuf;
  }

  public int dataSize() {
    return buf.position();
  }

  public ByteBuffer getByteBuffer() {
    return buf;
  }

  private void beforeIncrease(int inc) {
    int minCap = buf.position() + inc;
    if (minCap > buf.capacity()) {
      int newCap = buf.capacity();
      do {
        newCap = newCap << 1;
      } while (minCap > newCap);

      ByteBuffer newBuf = MemoryUtil.allocateDirect(newCap);
      MemoryUtil.copyMemory(
          MemoryUtil.getAddress(buf), MemoryUtil.getAddress(newBuf), buf.position());
      newBuf.position(buf.position());

      if (buf != initBuf) {
        MemoryUtil.free(buf);
      }

      buf = newBuf;
    }
  }

  public void put(CodecDataInput cdi, int len) {
    beforeIncrease(len);

    buf.limit(buf.position() + len);
    MemoryUtil.readFully(buf, cdi, len);
  }

  public void putByte(byte v) {
    beforeIncrease(1);

    buf.limit(buf.position() + 1);
    buf.put(v);
  }
}
