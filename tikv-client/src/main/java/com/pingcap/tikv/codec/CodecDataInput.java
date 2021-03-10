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

import static com.pingcap.tikv.codec.Codec.*;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.CodecException;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.annotation.Nonnull;

public class CodecDataInput implements DataInput {
  protected final DataInputStream inputStream;
  protected final UnSyncByteArrayInputStream backingStream;
  protected final byte[] backingBuffer;

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
  public void readFully(@Nonnull byte[] b) {
    try {
      inputStream.readFully(b);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void readFully(@Nonnull byte[] b, int off, int len) {
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
      byte[] readBuffer = new byte[2];
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
  @Nonnull
  public String readUTF() {
    try {
      return inputStream.readUTF();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * peek the first encoded value and return its length
   *
   * @return first encoded value
   */
  public int cutOne() {
    if (available() < 1) {
      throw new CodecException("invalid encoded key");
    }
    int flag = readByte();
    int a1 = this.available();

    switch (flag) {
      case NULL_FLAG:
      case INT_FLAG:
      case UINT_FLAG:
      case FLOATING_FLAG:
      case DURATION_FLAG:
        RealCodec.readDouble(this);
        break;
      case BYTES_FLAG:
        BytesCodec.readBytes(this);
        break;
      case COMPACT_BYTES_FLAG:
        BytesCodec.readCompactBytes(this);
        break;
      case DECIMAL_FLAG:
        DecimalCodec.readDecimal(this);
        break;
        // case VARINT_FLAG:
        //      l = peekVarint(b);
        // case UVARINT_FLAG:
        //        l = peekUvarint(b);
        // case JSON_FLAG:
        //        l = json.PeekBytesAsJSON(b);
      default:
        throw new CodecException("invalid encoded key flag " + flag);
    }
    int a2 = this.available();
    return a1 - a2 + 1;
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

  /**
   * An copy of ByteArrayInputStream without synchronization for faster decode.
   *
   * @see ByteArrayInputStream
   */
  private static class UnSyncByteArrayInputStream extends InputStream {
    protected byte[] buf;
    protected int pos;
    protected int mark = 0;
    protected int count;

    UnSyncByteArrayInputStream(byte[] buf) {
      this.buf = buf;
      this.pos = 0;
      this.count = buf.length;
    }

    @Override
    public int read() {
      return (pos < count) ? (buf[pos++] & 0xff) : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
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

    @Override
    public long skip(long n) {
      long k = count - pos;
      if (n < k) {
        k = n < 0 ? 0 : n;
      }

      pos += k;
      return k;
    }

    @Override
    public int available() {
      return count - pos;
    }

    @Override
    public boolean markSupported() {
      return true;
    }

    @Override
    public void mark(int readAheadLimit) {
      mark = pos;
    }

    @Override
    public void reset() {
      pos = mark;
    }

    @Override
    public void close() throws IOException {}
  }
}
