/*
 *
 * Copyright 2018 PingCAP, Inc.
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

package com.pingcap.tikv.codec;

import static java.util.Objects.requireNonNull;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.TiClientInternalException;
import gnu.trove.list.array.TByteArrayList;
import io.netty.handler.codec.CodecException;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import sun.misc.Unsafe;

public class CodecDataInput {
  private static final Unsafe UNSAFE;
  private static final int BYTE_ARRAY_BASE_OFFSET;
  private static final boolean IS_LITTLE_ENDIAN;

  static {
    try {
      Field field = Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      UNSAFE = (Unsafe)field.get(null);
      BYTE_ARRAY_BASE_OFFSET = UNSAFE.arrayBaseOffset(byte[].class);
      IS_LITTLE_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);
    } catch (Exception e) {
      throw new TiClientInternalException("Unable to use Java Unsafe Package", e);
    }
  }

  private final byte[] backingBuffer;
  private int pos;
  private final int count;
  private int mark;

  private void checkLength(int required) {
    if (backingBuffer.length - pos < required) {
      throw new CodecException("EOF reached");
    }
  }

  public CodecDataInput(ByteString data) {
    this(data.toByteArray());
  }

  public CodecDataInput(byte[] buf) {
    backingBuffer = requireNonNull(buf, "buf is null");
    count = backingBuffer.length;
    pos = 0;
    mark = 0;
  }

  void readFully(byte[] b, int len) {
    requireNonNull(b, "buf is null");
    System.arraycopy(backingBuffer, pos, b, 0, len);
    pos += len;
  }

  public void skipBytes(int n) {
    pos += n;
  }

  byte readByte() {
    byte b = backingBuffer[pos];
    pos ++;
    return b;
  }

  public int readUnsignedByte() {
    int r = backingBuffer[pos] & 0xFF;
    pos ++;
    return r;
  }

  public int readPartialUnsignedShort() {
    if (count > pos) {
      short r = UNSAFE.getShort(backingBuffer, (long) pos + BYTE_ARRAY_BASE_OFFSET);
      if (IS_LITTLE_ENDIAN) {
        r = Short.reverseBytes(r);
      }
      pos += 2;
      return r & 0xFFFF;
    }

    if (count - pos == 1) {
      int r = (backingBuffer[pos] & 0xFF) << 8;
      pos ++;
      return r;
    }

    return 0;
  }

  long readLong() {
    checkLength(8);
    long r = UNSAFE.getLong(backingBuffer, (long)pos + BYTE_ARRAY_BASE_OFFSET);
    if (IS_LITTLE_ENDIAN) {
      r = Long.reverseBytes(r);
    }
    pos += 8;
    return r;
  }

  final long readPartialLong() {
    if (available() >= 8) {
      return readLong();
    }

    int shift = 56;
    long r = 0;
    if (available() > 0) {
      r += (long) backingBuffer[pos] << shift;
      pos++;
      shift -= 8;
    }

    while (available() > 0 && shift >= 0) {
      r += (long) (backingBuffer[pos] & 0xFF) << shift;
      pos++;
      shift -= 8;
    }
    return r;
  }

  public static final int GRP_SIZE = 8;
  public static final int MARKER = 0xFF;
  public static final byte[] PADS = new byte[GRP_SIZE];

  // readBytes decodes bytes which is encoded by EncodeBytes before,
  // returns the leftover bytes and decoded value if no error.
  public byte[] readBytes() {
    TByteArrayList cdo = new TByteArrayList(Math.min(available(), 256));
    while (true) {
      int padCount;
      int marker = backingBuffer[pos + GRP_SIZE] & 0xff;
      int curPos = pos;
      pos += GRP_SIZE + 1;
      padCount = MARKER - marker;

      if (padCount > GRP_SIZE) {
        throw new IllegalArgumentException("Wrong padding count");
      }
      int realGroupSize = GRP_SIZE - padCount;
      cdo.add(backingBuffer, curPos, realGroupSize);

      if (padCount != 0) {
        // Check validity of padding bytes.
        for (int i = realGroupSize; i < GRP_SIZE; i++) {
          if (backingBuffer[i + curPos] != 0) {
            throw new IllegalArgumentException();
          }
        }
        break;
      }
    }

    return cdo.toArray();
  }

  public int peekByte() {
    return backingBuffer[pos] & 0xFF;
  }

  public void mark(int givenPos) {
    mark = givenPos;
  }

  public void reset() {
    pos = mark;
  }

  public boolean eof() {
    return available() == 0;
  }

  public int size() {
    return count;
  }

  public int available() {
    return count - pos;
  }

  public byte[] toByteArray() {
    return backingBuffer;
  }
}
