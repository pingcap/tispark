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

package com.pingcap.tikv.codec;

import com.google.common.primitives.Longs;

public class CodecDataOutputLittleEndian extends CodecDataOutput {
  public CodecDataOutputLittleEndian() {
    super();
  }

  public CodecDataOutputLittleEndian(int size) {
    super(size);
  }

  @Override
  public void writeShort(int v) {
    try {
      s.write(v & 0xff);
      s.write((v >> 8) & 0xff);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeChar(int v) {
    writeShort(v);
  }

  @Override
  public void writeInt(int v) {
    try {
      s.write(v & 0xff);
      s.write((v >> 8) & 0xff);
      s.write((v >> 16) & 0xff);
      s.write((v >> 24) & 0xff);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeLong(long v) {
    byte[] bytes = Longs.toByteArray(Long.reverseBytes(v));
    write(bytes, 0, bytes.length);
  }

  @Override
  public void writeFloat(float v) {
    writeInt(Float.floatToIntBits(v));
  }

  @Override
  public void writeDouble(double v) {
    writeLong(Double.doubleToLongBits(v));
  }
}
