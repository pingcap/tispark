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

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.CodecException;
import javax.annotation.Nonnull;

public class CodecDataInputLittleEndian extends CodecDataInput {

  public CodecDataInputLittleEndian(ByteString data) {
    super(data);
  }

  public CodecDataInputLittleEndian(byte[] buf) {
    super(buf);
  }

  @Override
  public short readShort() {
    int ch1 = readUnsignedByte();
    int ch2 = readUnsignedByte();
    return (short) ((ch1) + (ch2 << 8));
  }

  @Override
  public int readUnsignedShort() {
    int ch1 = readUnsignedByte();
    int ch2 = readUnsignedByte();
    return (ch1) + (ch2 << 8);
  }

  @Override
  public char readChar() {
    int ch1 = readUnsignedByte();
    int ch2 = readUnsignedByte();
    return (char) ((ch1) + (ch2 << 8));
  }

  @Override
  public int readInt() {
    int ch1 = readUnsignedByte();
    int ch2 = readUnsignedByte();
    int ch3 = readUnsignedByte();
    int ch4 = readUnsignedByte();
    return ((ch1) + (ch2 << 8) + (ch3 << 16) + (ch4 << 24));
  }

  @Override
  public long readLong() {
    byte[] readBuffer = new byte[8];
    readFully(readBuffer, 0, 8);
    return ((readBuffer[0] & 0xff)
        + ((readBuffer[1] & 0xff) << 8)
        + ((readBuffer[2] & 0xff) << 16)
        + ((long) (readBuffer[3] & 0xff) << 24)
        + ((long) (readBuffer[4] & 0xff) << 32)
        + ((long) (readBuffer[5] & 0xff) << 40)
        + ((long) (readBuffer[6] & 0xff) << 48)
        + ((long) (readBuffer[7] & 0xff) << 56));
  }

  @Override
  public float readFloat() {
    return Float.intBitsToFloat(readInt());
  }

  @Override
  public double readDouble() {
    return Double.longBitsToDouble(readLong());
  }

  @Override
  public String readLine() {
    throw new CodecException("unimplemented");
  }

  @Override
  @Nonnull
  public String readUTF() {
    throw new CodecException("unimplemented");
  }

  public int peekByte() {
    return super.peekByte();
  }

  public int currentPos() {
    return super.currentPos();
  }

  public void mark(int givenPos) {
    super.mark(givenPos);
  }

  public void reset() {
    super.reset();
  }

  public boolean eof() {
    return super.eof();
  }

  public int size() {
    return super.size();
  }

  public int available() {
    return super.available();
  }

  public byte[] toByteArray() {
    return super.toByteArray();
  }
}
