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
}
