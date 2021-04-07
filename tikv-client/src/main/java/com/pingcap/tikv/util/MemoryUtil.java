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

package com.pingcap.tikv.util;

import com.google.common.primitives.UnsignedLong;
import com.pingcap.tikv.codec.CodecDataInput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

// Copied from io.indexr.util.MemoryUtil.java with some modifications.
public class MemoryUtil {
  public static final ByteBuffer EMPTY_BYTE_BUFFER = allocate(0);

  public static BigDecimal getDecimal32(ByteBuffer data, int offset, int scale) {
    int n = data.getInt(offset);
    BigInteger dec = BigInteger.valueOf(n);
    return new BigDecimal(dec, scale);
  }

  public static BigDecimal getDecimal64(ByteBuffer data, int offset, int scale) {
    long n = data.getLong(offset);
    BigInteger dec = BigInteger.valueOf(n);
    return new BigDecimal(dec, scale);
  }

  public static BigDecimal getDecimal128(ByteBuffer data, int offset, int scale) {
    UnsignedLong n0 = UnsignedLong.fromLongBits(data.getLong(offset));
    long n1 = data.getLong(offset + 8);

    BigInteger dec = BigInteger.valueOf(n1);
    dec = dec.shiftLeft(64).add(n0.bigIntegerValue());
    return new BigDecimal(dec, scale);
  }

  public static BigDecimal getDecimal256(ByteBuffer data, int offset, int scale) {
    int limbs = data.getShort(offset + 32);
    BigInteger dec = BigInteger.ZERO;
    for (int i = limbs - 1; i >= 0; i--) {
      UnsignedLong d = UnsignedLong.fromLongBits(data.getLong(offset + i * 8));
      dec = dec.shiftLeft(64).add(d.bigIntegerValue());
    }
    int sign = data.get(offset + 34);
    BigDecimal result = new BigDecimal(dec, scale);
    if (sign > 0) {
      return result.negate();
    }
    return result;
  }

  public static ByteBuffer allocate(int cap) {
    ByteBuffer bb = ByteBuffer.allocate(cap);
    // It make operation faster, but kill the cross platform ability.
    bb.order(ByteOrder.nativeOrder());
    return bb;
  }

  public static ByteBuffer copyOf(ByteBuffer buf, int newCap) {
    ByteBuffer newBuf = ByteBuffer.wrap(Arrays.copyOf(buf.array(), newCap));
    newBuf.position(buf.position());
    return newBuf;
  }

  public static void readFully(ByteBuffer dst, CodecDataInput cdi, int length) {
    int remaining;
    for (remaining = length; remaining > 0 && !cdi.eof(); remaining--) {
      byte b = cdi.readByte();
      dst.put(b);
    }
    dst.position(dst.position() + remaining);
  }
}
