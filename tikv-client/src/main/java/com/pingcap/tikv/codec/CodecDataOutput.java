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

package com.pingcap.tikv.codec;

import com.google.protobuf.ByteString;
import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

// A trivial implementation supposed to be replaced
public class CodecDataOutput implements DataOutput {
  protected final DataOutputStream s;
  // TODO: Switch to ByteBuffer if possible, or a chain of ByteBuffer
  protected final ByteArrayOutputStream byteArray;

  public CodecDataOutput() {
    byteArray = new ByteArrayOutputStream();
    s = new DataOutputStream(byteArray);
  }

  public CodecDataOutput(int size) {
    byteArray = new ByteArrayOutputStream(size);
    s = new DataOutputStream(byteArray);
  }

  @Override
  public void write(int b) {
    try {
      s.write(b);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(byte[] b) {
    try {
      s.write(b);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(byte[] b, int off, int len) {
    try {
      s.write(b, off, len);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeBoolean(boolean v) {
    try {
      s.writeBoolean(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeByte(int v) {
    try {
      s.writeByte(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeShort(int v) {
    try {
      s.writeShort(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeChar(int v) {
    try {
      s.writeChar(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeInt(int v) {
    try {
      s.writeInt(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeLong(long v) {
    try {
      s.writeLong(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeFloat(float v) {
    try {
      s.writeFloat(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeDouble(double v) {
    try {
      s.writeDouble(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeBytes(String v) {
    try {
      s.writeBytes(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeChars(String v) {
    try {
      s.writeChars(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void writeUTF(String v) {
    try {
      s.writeUTF(v);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] toBytes() {
    return byteArray.toByteArray();
  }

  public ByteString toByteString() {
    return ByteString.copyFrom(byteArray.toByteArray());
  }

  public int size() {
    return this.byteArray.size();
  }

  public void reset() {
    this.byteArray.reset();
  }
}
