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
import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Cleaner;
import sun.misc.Unsafe;
import sun.nio.ch.DirectBuffer;

// Copied from io.indexr.util.MemoryUtil.java with some modifications.
public class MemoryUtil {
  public static final ByteBuffer EMPTY_BYTE_BUFFER_DIRECT = allocateDirect(0);
  public static final Unsafe unsafe;
  private static final Logger logger = LoggerFactory.getLogger(MemoryUtil.class);
  private static final long UNSAFE_COPY_THRESHOLD = 1024 * 1024L; // copied from java.nio.Bits
  private static final Class<?> DIRECT_BYTE_BUFFER_CLASS;
  private static final long DIRECT_BYTE_BUFFER_ADDRESS_OFFSET;
  private static final long DIRECT_BYTE_BUFFER_CAPACITY_OFFSET;
  private static final long DIRECT_BYTE_BUFFER_LIMIT_OFFSET;
  private static final long DIRECT_BYTE_BUFFER_POSITION_OFFSET;
  private static final long DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET;
  private static final long DIRECT_BYTE_BUFFER_CLEANER;
  private static final Class<?> BYTE_BUFFER_CLASS;
  private static final long BYTE_BUFFER_OFFSET_OFFSET;
  private static final long BYTE_BUFFER_HB_OFFSET;
  private static final long BYTE_ARRAY_BASE_OFFSET;

  private static final long STRING_VALUE_OFFSET;

  private static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
  private static int PAGE_SIZE = -1;

  private static boolean HDFS_READ_HACK_ENABLE;
  private static long BlockReaderLocal_verifyChecksum;
  private static long BlockReaderLocal_dataIn;
  private static long DFSInputStream_verifyChecksum;
  private static long DFSInputStream_blockReader;

  static {
    // We support all in fact.
    // if (BIG_ENDIAN) {
    //    throw new RuntimeException("We only support littel endian platform!");
    // }
    try {
      Field field = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
      field.setAccessible(true);
      unsafe = (sun.misc.Unsafe) field.get(null);

      Class<?> clazz = ByteBuffer.allocateDirect(0).getClass();
      DIRECT_BYTE_BUFFER_ADDRESS_OFFSET =
          unsafe.objectFieldOffset(Buffer.class.getDeclaredField("address"));
      DIRECT_BYTE_BUFFER_CAPACITY_OFFSET =
          unsafe.objectFieldOffset(Buffer.class.getDeclaredField("capacity"));
      DIRECT_BYTE_BUFFER_LIMIT_OFFSET =
          unsafe.objectFieldOffset(Buffer.class.getDeclaredField("limit"));
      DIRECT_BYTE_BUFFER_POSITION_OFFSET =
          unsafe.objectFieldOffset(Buffer.class.getDeclaredField("position"));
      DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET =
          unsafe.objectFieldOffset(clazz.getDeclaredField("att"));
      DIRECT_BYTE_BUFFER_CLEANER = unsafe.objectFieldOffset(clazz.getDeclaredField("cleaner"));
      DIRECT_BYTE_BUFFER_CLASS = clazz;

      clazz = ByteBuffer.allocate(0).getClass();
      BYTE_BUFFER_OFFSET_OFFSET =
          unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("offset"));
      BYTE_BUFFER_HB_OFFSET = unsafe.objectFieldOffset(ByteBuffer.class.getDeclaredField("hb"));
      BYTE_BUFFER_CLASS = clazz;

      BYTE_ARRAY_BASE_OFFSET = unsafe.arrayBaseOffset(byte[].class);

      STRING_VALUE_OFFSET =
          MemoryUtil.unsafe.objectFieldOffset(String.class.getDeclaredField("value"));

      try {
        BlockReaderLocal_verifyChecksum =
            unsafe.objectFieldOffset(
                Class.forName("org.apache.hadoop.hdfs.BlockReaderLocal")
                    .getDeclaredField("verifyChecksum"));
        BlockReaderLocal_dataIn =
            unsafe.objectFieldOffset(
                Class.forName("org.apache.hadoop.hdfs.BlockReaderLocal")
                    .getDeclaredField("dataIn"));

        DFSInputStream_verifyChecksum =
            unsafe.objectFieldOffset(
                Class.forName("org.apache.hadoop.hdfs.DFSInputStream")
                    .getDeclaredField("verifyChecksum"));
        DFSInputStream_blockReader =
            unsafe.objectFieldOffset(
                Class.forName("org.apache.hadoop.hdfs.DFSInputStream")
                    .getDeclaredField("blockReader"));

        HDFS_READ_HACK_ENABLE = true;
      } catch (Exception e) {
        HDFS_READ_HACK_ENABLE = false;
        logger.warn("hdfs read hack is off because of error: {}", e.getCause());
      }
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  public static boolean getBlockReaderLocal_verifyChecksum(Object br) {
    return unsafe.getBoolean(br, BlockReaderLocal_verifyChecksum);
  }

  public static void setBlockReaderLocal_verifyChecksum(Object br, boolean value) {
    unsafe.putBoolean(br, BlockReaderLocal_verifyChecksum, value);
  }

  public static FileChannel getBlockReaderLocal_dataIn(Object br) {
    return (FileChannel) unsafe.getObject(br, BlockReaderLocal_dataIn);
  }

  public static int pageSize() {
    if (PAGE_SIZE == -1) {
      PAGE_SIZE = unsafe.pageSize();
    }
    return PAGE_SIZE;
  }

  public static void setMemory(long addr, long size, byte v) {
    unsafe.setMemory(addr, size, v);
  }

  public static void setMemory(Object base, long offset, long size, byte v) {
    unsafe.setMemory(base, offset, size, v);
  }

  public static long getAddress(ByteBuffer buffer) {
    assert buffer.getClass() == DIRECT_BYTE_BUFFER_CLASS;
    return unsafe.getLong(buffer, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET);
  }

  public static int getCap(ByteBuffer buffer) {
    assert buffer.getClass() == DIRECT_BYTE_BUFFER_CLASS;
    return unsafe.getInt(buffer, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET);
  }

  public static long allocate(long size) {
    // return Native.malloc(size);
    return unsafe.allocateMemory(size);
  }

  public static void free(long addr) {
    // Native.free(addr);
    unsafe.freeMemory(addr);
  }

  /** Good manner to free a buffer before forget it. */
  public static void free(ByteBuffer buffer) {
    if (buffer.isDirect()) {
      Cleaner cleaner = ((DirectBuffer) buffer).cleaner();
      if (cleaner != null) {
        cleaner.clean();
      }
    }
  }

  public static void setByte(long address, byte b) {
    unsafe.putByte(address, b);
  }

  public static void setShort(long address, short s) {
    unsafe.putShort(address, s);
  }

  public static void setInt(long address, int l) {
    unsafe.putInt(address, l);
  }

  public static void setDecimal(long address, BigDecimal v, int scale) {
    BigDecimal bigDec = v;
    BigInteger bigInt = bigDec.scaleByPowerOfTen(scale).toBigInteger();
    byte[] arr = bigInt.toByteArray();
    for (int i = 0; i < arr.length; i++) {
      unsafe.putByte(address + i, arr[arr.length - 1 - i]);
    }
  }

  public static void setDecimal256(long address, BigDecimal v, int scale) {
    BigDecimal bigDec = v;
    BigInteger bigInt = bigDec.scaleByPowerOfTen(scale).toBigInteger();
    int sign = 0;
    if (bigInt.signum() < 0) {
      sign = 1;
      bigInt = bigInt.abs();
    }
    byte[] arr = bigInt.toByteArray();
    if (arr.length > 32) {
      throw new RuntimeException("The inserting decimal is out of range: " + v.toString());
    }
    int limbs = arr.length / 8;
    if (arr.length % 8 > 0) {
      limbs++;
    }

    for (int i = 0; i < arr.length; i++) {
      unsafe.putByte(address + i, arr[arr.length - 1 - i]);
    }
    unsafe.putShort(address + 32, (short) limbs);
    unsafe.putShort(address + 34, (short) sign);
  }

  public static void setLong(long address, long l) {
    unsafe.putLong(address, l);
  }

  public static void setFloat(long address, float v) {
    unsafe.putFloat(address, v);
  }

  public static void setDouble(long address, double v) {
    unsafe.putDouble(address, v);
  }

  public static byte getByte(long address) {
    return unsafe.getByte(address);
  }

  public static short getShort(long address) {
    return (short) (unsafe.getShort(address) & 0xffff);
  }

  public static int getInt(long address) {
    return unsafe.getInt(address);
  }

  public static BigDecimal getDecimal32(long address, int scale) {
    int n = getInt(address);
    BigInteger dec = BigInteger.valueOf(n);
    return new BigDecimal(dec, scale);
  }

  public static BigDecimal getDecimal64(long address, int scale) {
    long n = getLong(address);
    BigInteger dec = BigInteger.valueOf(n);
    return new BigDecimal(dec, scale);
  }

  public static BigDecimal getDecimal128(long address, int scale) {
    UnsignedLong n0 = UnsignedLong.fromLongBits(getLong(address));
    long n1 = getLong(address + 8);

    BigInteger dec = BigInteger.valueOf(n1);
    dec = dec.shiftLeft(64).add(n0.bigIntegerValue());
    return new BigDecimal(dec, scale);
  }

  public static BigDecimal getDecimal256(long address, int scale) {
    int limbs = getShort(address + 32);
    BigInteger dec = BigInteger.ZERO;
    for (int i = limbs - 1; i >= 0; i--) {
      UnsignedLong d = UnsignedLong.fromLongBits(unsafe.getLong(address + i * 8));
      dec = dec.shiftLeft(64).add(d.bigIntegerValue());
    }
    int sign = unsafe.getByte(address + 34);
    BigDecimal result = new BigDecimal(dec, scale);
    if (sign > 0) {
      return result.negate();
    }
    return result;
  }

  public static long getLong(long address) {
    return unsafe.getLong(address);
  }

  public static float getFloat(long address) {
    return unsafe.getFloat(address);
  }

  public static double getDouble(long address) {
    return unsafe.getDouble(address);
  }

  public static ByteBuffer getByteBuffer(long address, int length, boolean autoFree) {
    ByteBuffer instance = getHollowDirectByteBuffer();
    if (autoFree) {
      Cleaner cleaner = Cleaner.create(instance, new Deallocator(address));
      setByteBuffer(instance, address, length, cleaner);
    } else {
      setByteBuffer(instance, address, length, null);
    }
    instance.order(ByteOrder.nativeOrder());
    return instance;
  }

  public static ByteBuffer getHollowDirectByteBuffer() {
    ByteBuffer instance;
    try {
      instance = (ByteBuffer) unsafe.allocateInstance(DIRECT_BYTE_BUFFER_CLASS);
    } catch (InstantiationException e) {
      throw new AssertionError(e);
    }
    instance.order(ByteOrder.nativeOrder());
    return instance;
  }

  public static void setByteBuffer(ByteBuffer instance, long address, int length, Cleaner cleaner) {
    unsafe.putLong(instance, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET, address);
    unsafe.putInt(instance, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET, length);
    unsafe.putInt(instance, DIRECT_BYTE_BUFFER_LIMIT_OFFSET, length);
    if (cleaner != null) {
      unsafe.putObject(instance, DIRECT_BYTE_BUFFER_CLEANER, cleaner);
    }
  }

  public static Object getAttachment(ByteBuffer instance) {
    assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
    return unsafe.getObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET);
  }

  public static void setAttachment(ByteBuffer instance, Object next) {
    assert instance.getClass() == DIRECT_BYTE_BUFFER_CLASS;
    unsafe.putObject(instance, DIRECT_BYTE_BUFFER_ATTACHMENT_OFFSET, next);
  }

  public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source, ByteBuffer hollowBuffer) {
    assert source.getClass() == DIRECT_BYTE_BUFFER_CLASS;
    unsafe.putLong(
        hollowBuffer,
        DIRECT_BYTE_BUFFER_ADDRESS_OFFSET,
        unsafe.getLong(source, DIRECT_BYTE_BUFFER_ADDRESS_OFFSET));
    unsafe.putInt(
        hollowBuffer,
        DIRECT_BYTE_BUFFER_POSITION_OFFSET,
        unsafe.getInt(source, DIRECT_BYTE_BUFFER_POSITION_OFFSET));
    unsafe.putInt(
        hollowBuffer,
        DIRECT_BYTE_BUFFER_LIMIT_OFFSET,
        unsafe.getInt(source, DIRECT_BYTE_BUFFER_LIMIT_OFFSET));
    unsafe.putInt(
        hollowBuffer,
        DIRECT_BYTE_BUFFER_CAPACITY_OFFSET,
        unsafe.getInt(source, DIRECT_BYTE_BUFFER_CAPACITY_OFFSET));
    return hollowBuffer;
  }

  public static ByteBuffer duplicateDirectByteBuffer(ByteBuffer source) {
    return duplicateDirectByteBuffer(source, getHollowDirectByteBuffer());
  }

  public static long getLongByByte(long address) {
    if (BIG_ENDIAN) {
      return (((long) unsafe.getByte(address)) << 56)
          | (((long) unsafe.getByte(address + 1) & 0xff) << 48)
          | (((long) unsafe.getByte(address + 2) & 0xff) << 40)
          | (((long) unsafe.getByte(address + 3) & 0xff) << 32)
          | (((long) unsafe.getByte(address + 4) & 0xff) << 24)
          | (((long) unsafe.getByte(address + 5) & 0xff) << 16)
          | (((long) unsafe.getByte(address + 6) & 0xff) << 8)
          | (((long) unsafe.getByte(address + 7) & 0xff));
    } else {
      return (((long) unsafe.getByte(address + 7)) << 56)
          | (((long) unsafe.getByte(address + 6) & 0xff) << 48)
          | (((long) unsafe.getByte(address + 5) & 0xff) << 40)
          | (((long) unsafe.getByte(address + 4) & 0xff) << 32)
          | (((long) unsafe.getByte(address + 3) & 0xff) << 24)
          | (((long) unsafe.getByte(address + 2) & 0xff) << 16)
          | (((long) unsafe.getByte(address + 1) & 0xff) << 8)
          | (((long) unsafe.getByte(address) & 0xff));
    }
  }

  public static int getIntByByte(long address) {
    if (BIG_ENDIAN) {
      return (((int) unsafe.getByte(address)) << 24)
          | (((int) unsafe.getByte(address + 1) & 0xff) << 16)
          | (((int) unsafe.getByte(address + 2) & 0xff) << 8)
          | (((int) unsafe.getByte(address + 3) & 0xff));
    } else {
      return (((int) unsafe.getByte(address + 3)) << 24)
          | (((int) unsafe.getByte(address + 2) & 0xff) << 16)
          | (((int) unsafe.getByte(address + 1) & 0xff) << 8)
          | (((int) unsafe.getByte(address) & 0xff));
    }
  }

  public static int getShortByByte(long address) {
    if (BIG_ENDIAN) {
      return (((int) unsafe.getByte(address)) << 8) | (((int) unsafe.getByte(address + 1) & 0xff));
    } else {
      return (((int) unsafe.getByte(address + 1)) << 8) | (((int) unsafe.getByte(address) & 0xff));
    }
  }

  public static void putLongByByte(long address, long value) {
    if (BIG_ENDIAN) {
      unsafe.putByte(address, (byte) (value >> 56));
      unsafe.putByte(address + 1, (byte) (value >> 48));
      unsafe.putByte(address + 2, (byte) (value >> 40));
      unsafe.putByte(address + 3, (byte) (value >> 32));
      unsafe.putByte(address + 4, (byte) (value >> 24));
      unsafe.putByte(address + 5, (byte) (value >> 16));
      unsafe.putByte(address + 6, (byte) (value >> 8));
      unsafe.putByte(address + 7, (byte) (value));
    } else {
      unsafe.putByte(address + 7, (byte) (value >> 56));
      unsafe.putByte(address + 6, (byte) (value >> 48));
      unsafe.putByte(address + 5, (byte) (value >> 40));
      unsafe.putByte(address + 4, (byte) (value >> 32));
      unsafe.putByte(address + 3, (byte) (value >> 24));
      unsafe.putByte(address + 2, (byte) (value >> 16));
      unsafe.putByte(address + 1, (byte) (value >> 8));
      unsafe.putByte(address, (byte) (value));
    }
  }

  public static void putIntByByte(long address, int value) {
    if (BIG_ENDIAN) {
      unsafe.putByte(address, (byte) (value >> 24));
      unsafe.putByte(address + 1, (byte) (value >> 16));
      unsafe.putByte(address + 2, (byte) (value >> 8));
      unsafe.putByte(address + 3, (byte) (value));
    } else {
      unsafe.putByte(address + 3, (byte) (value >> 24));
      unsafe.putByte(address + 2, (byte) (value >> 16));
      unsafe.putByte(address + 1, (byte) (value >> 8));
      unsafe.putByte(address, (byte) (value));
    }
  }

  public static void setBytes(long address, ByteBuffer buffer) {
    int start = buffer.position();
    int count = buffer.limit() - start;
    if (count == 0) return;

    if (buffer.isDirect()) setBytes(((DirectBuffer) buffer).address() + start, address, count);
    else setBytes(address, buffer.array(), buffer.arrayOffset() + start, count);
  }

  /**
   * Transfers objCount bytes from buffer to Memory
   *
   * @param address start offset in the memory
   * @param buffer the data buffer
   * @param bufferOffset start offset of the buffer
   * @param count number of bytes to transfer
   */
  public static void setBytes(long address, byte[] buffer, int bufferOffset, int count) {
    assert buffer != null;
    assert !(bufferOffset < 0 || count < 0 || bufferOffset + count > buffer.length);
    setBytes(buffer, bufferOffset, address, count);
  }

  public static void setBytes(long src, long trg, long count) {
    while (count > 0) {
      long size = Math.min(count, UNSAFE_COPY_THRESHOLD);
      unsafe.copyMemory(src, trg, size);
      count -= size;
      src += size;
      trg += size;
    }
  }

  public static void setBytes(byte[] src, int offset, long trg, long count) {
    while (count > 0) {
      long size = (count > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : count;
      unsafe.copyMemory(src, BYTE_ARRAY_BASE_OFFSET + offset, null, trg, size);
      count -= size;
      offset += size;
      trg += size;
    }
  }

  /**
   * Transfers objCount bytes from Memory starting at memoryOffset to buffer starting at
   * bufferOffset
   *
   * @param address start offset in the memory
   * @param buffer the data buffer
   * @param bufferOffset start offset of the buffer
   * @param count number of bytes to transfer
   */
  public static void getBytes(long address, byte[] buffer, int bufferOffset, int count) {
    if (buffer == null) throw new NullPointerException();
    else if (bufferOffset < 0 || count < 0 || count > buffer.length - bufferOffset)
      throw new IndexOutOfBoundsException();
    else if (count == 0) return;

    unsafe.copyMemory(null, address, buffer, BYTE_ARRAY_BASE_OFFSET + bufferOffset, count);
  }

  public static char[] getStringValue(String str) {
    return (char[]) MemoryUtil.unsafe.getObject(str, STRING_VALUE_OFFSET);
  }

  public static void copyMemory(long fromAddr, long toAddr, int count) {
    copyMemory(null, fromAddr, null, toAddr, count);
  }

  public static void copyMemory(
      Object src, long srcOffset, Object dst, long dstOffset, long length) {
    // Check if dstOffset is before or after srcOffset to determine if we should copy
    // forward or backwards. This is necessary in case src and dst overlap.
    if (dstOffset < srcOffset) {
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
        srcOffset += size;
        dstOffset += size;
      }
    } else {
      srcOffset += length;
      dstOffset += length;
      while (length > 0) {
        long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
        srcOffset -= size;
        dstOffset -= size;
        unsafe.copyMemory(src, srcOffset, dst, dstOffset, size);
        length -= size;
      }
    }
  }

  public static long getTotalPhysicalMemorySize() {
    return ((OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean())
        .getTotalPhysicalMemorySize();
  }

  public static ByteBuffer allocateDirect(int cap) {
    ByteBuffer bb = ByteBuffer.allocateDirect(cap);
    // It make operation faster, but kill the cross platform ability.
    bb.order(ByteOrder.nativeOrder());
    return bb;
  }

  public static void readFully(ByteBuffer dst, CodecDataInput cdi, int length) {
    // read bytes from cdi to buffer(off-heap)
    long disAddr = MemoryUtil.getAddress(dst);
    long bufPos = dst.position();
    for (int i = 0; i < length && !cdi.eof(); i++) {
      byte b = cdi.readByte();
      MemoryUtil.setByte(disAddr + bufPos + i, b);
    }
    dst.position((int) (bufPos + length));
  }

  private static class Deallocator implements Runnable {
    private long address;

    private Deallocator(long address) {
      assert (address != 0);
      this.address = address;
    }

    @Override
    public void run() {
      if (address == 0) {
        return;
      }
      free(address);
      address = 0;
    }
  }
}
