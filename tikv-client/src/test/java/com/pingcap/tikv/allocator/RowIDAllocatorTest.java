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

package com.pingcap.tikv.allocator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class RowIDAllocatorTest {

  @Test
  public void TestShardRowBitsOverflow() {
    assertTrue(RowIDAllocator.shardRowBitsOverflow(0, 0xffffffffffffffL, 8, true));
    assertFalse(RowIDAllocator.shardRowBitsOverflow(0, 0xffffffffffffffL, 7, true));
    assertTrue(RowIDAllocator.shardRowBitsOverflow(0, 0x8000000000000000L, 2, false));
    assertFalse(RowIDAllocator.shardRowBitsOverflow(0, 0xff, 64 - 8, false));
  }

  @Test
  public void TestGetShardRowId() {
    {
      long maxShardRowIDBits = 1L;
      long partitionIndex = 0L;
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000000000000000L),
          0x0000000000000000L);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0fffffffffffffffL),
          0x0fffffffffffffffL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x10000000000000efL),
          0x10000000000000efL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x200000000000000fL),
          0x200000000000000fL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x3000000000000000L),
          0x3000000000000000L);
    }

    {
      long maxShardRowIDBits = 1L;
      long partitionIndex = 1L;
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000000000000000L),
          0x4000000000000000L);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0fffffffffffffffL),
          0x4fffffffffffffffL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x10000000000000efL),
          0x50000000000000efL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x200000000000000fL),
          0x600000000000000fL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x3000000000000000L),
          0x7000000000000000L);
    }

    {
      long maxShardRowIDBits = 15L;
      long partitionIndex = 0L;
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000000000000000L),
          0x0000000000000000L);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x0000ffffffffffffL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x00001000000000efL),
          0x00001000000000efL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x0000ffffffffffffL);
    }

    {
      long maxShardRowIDBits = 15L;
      long partitionIndex = 1L;
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000000000000000L),
          0x0001000000000000L);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x0001ffffffffffffL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x00001000000000efL),
          0x00011000000000efL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x0001ffffffffffffL);
    }

    {
      long maxShardRowIDBits = 15L;
      long partitionIndex = (1L << 15) - 2;
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000000000000000L),
          0x7ffe000000000000L);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x7ffeffffffffffffL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x00001000000000efL),
          0x7ffe1000000000efL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x7ffeffffffffffffL);
    }

    {
      long maxShardRowIDBits = 15L;
      long partitionIndex = (1L << 15) - 1;
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000000000000000L),
          0x7fff000000000000L);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x7fffffffffffffffL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x00001000000000efL),
          0x7fff1000000000efL);
      assertEquals(
          RowIDAllocator.getShardRowId(maxShardRowIDBits, partitionIndex, 0x0000ffffffffffffL),
          0x7fffffffffffffffL);
    }
  }
}
