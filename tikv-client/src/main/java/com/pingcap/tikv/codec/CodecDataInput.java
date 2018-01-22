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

import java.io.*;

public class CodecDataInput implements DataInput {
  /**
   * An copy of ByteArrayInputStream without synchronization for faster decode.
   *
   * @see ByteArrayInputStream
   */
  private class UnSyncByteArrayInputStream extends InputStream {
    protected byte buf[];
    protected int pos;
    protected int mark = 0;
    protected int count;

    UnSyncByteArrayInputStream(byte buf[]) {
      this.buf = buf;
      this.pos = 0;
      this.count = buf.length;
    }

    public UnSyncByteArrayInputStream(byte buf[], int offset, int length) {
      this.buf = buf;
      this.pos = offset;
      this.count = Math.min(offset + length, buf.length);
      this.mark = offset;
    }

    public int read() {
      return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    public int read(byte b[], int off, int len) {
      if (b == null) {
        throw new NullPointerException();
      } else if (off < 0 || len < 0 || len > b.length - off) {
        throw new IndexOutOfBoundsException();
      }

      if (pos >= count) {
        return -1;
      }

      int avail = count - pos;
      if (len > avail) {
        len = avail;
      }
      if (len <= 0) {
        return 0;
      }
      System.arraycopy(buf, pos, b, off, len);
      pos += len;
      return len;
    }

    public long skip(long n) {
      long k = count - pos;
      if (n < k) {
        k = n < 0 ? 0 : n;
      }

      pos += k;
      return k;
    }

    public int available() {
      return count - pos;
    }
    public boolean markSupported() {
      return true;
    }

    public void mark(int readAheadLimit) {
      mark = pos;
    }

    public void reset() {
      pos = mark;
    }

    public void close() throws IOException {
    }
  }
  private final DataInputStream inputStream;
  private final UnSyncByteArrayInputStream backingStream;
  private final byte[] backingBuffer;

  public CodecDataInput(ByteString data) {
    this(data.toByteArray());
  }

  public CodecDataInput(byte[] buf) {
    backingBuffer = buf;
    // MyDecimal usually will consume more bytes. If this happened,
    // we need have a mechanism to reset backingStream.
    // User mark first and then reset it later can do the trick.
    backingStream =
        new UnSyncByteArrayInputStream(buf) {
          @Override
          public void mark(int givenPos) {
            mark = givenPos;
          }
        };
    inputStream = new DataInputStream(backingStream);
  }

  @Override
  public void readFully(byte[] b) {
    try {
      inputStream.readFully(b);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFully(byte[] b, int off, int len) {
    try {
      inputStream.readFully(b, off, len);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int skipBytes(int n) {
    try {
      return inputStream.skipBytes(n);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean readBoolean() {
    try {
      return inputStream.readBoolean();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte readByte() {
    try {
      return inputStream.readByte();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readUnsignedByte() {
    try {
      return inputStream.readUnsignedByte();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public short readShort() {
    try {
      return inputStream.readShort();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readUnsignedShort() {
    try {
      return inputStream.readUnsignedShort();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int readPartialUnsignedShort() {
    try {
      byte readBuffer[] = new byte[2];
      inputStream.read(readBuffer, 0, 2);
      return ((readBuffer[0] & 0xff) << 8) + ((readBuffer[1] & 0xff) << 0);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public char readChar() {
    try {
      return inputStream.readChar();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int readInt() {
    try {
      return inputStream.readInt();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public long readLong() {
    try {
      return inputStream.readLong();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public final long readPartialLong() {
    try {
      byte readBuffer[] = new byte[8];
      inputStream.read(readBuffer, 0, 8);
      return (((long) readBuffer[0] << 56) +
          ((long) (readBuffer[1] & 255) << 48) +
          ((long) (readBuffer[2] & 255) << 40) +
          ((long) (readBuffer[3] & 255) << 32) +
          ((long) (readBuffer[4] & 255) << 24) +
          ((readBuffer[5] & 255) << 16) +
          ((readBuffer[6] & 255) << 8) +
          ((readBuffer[7] & 255) << 0));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public float readFloat() {
    try {
      return inputStream.readFloat();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public double readDouble() {
    try {
      return inputStream.readDouble();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String readLine() {
    try {
      return inputStream.readLine();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public String readUTF() {
    try {
      return inputStream.readUTF();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public int peekByte() {
    mark(currentPos());
    int b = readByte() & 0xFF;
    reset();
    return b;
  }

  public int currentPos() {
    return size() - available();
  }

  public void mark(int givenPos) {
    this.backingStream.mark(givenPos);
  }

  public void reset() {
    this.backingStream.reset();
  }

  public boolean eof() {
    return backingStream.available() == 0;
  }

  public int size() {
    return backingBuffer.length;
  }

  public int available() {
    return backingStream.available();
  }

  public byte[] toByteArray() {
    return backingBuffer;
  }
}
