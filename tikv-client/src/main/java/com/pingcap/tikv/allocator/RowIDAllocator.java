/*
 * Copyright 2019 PingCAP, Inc.
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

import com.google.common.primitives.UnsignedLongs;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.exception.TiBatchWriteException;

/**
 * RowIDAllocator read current start from TiKV and write back 'start+step' back to TiKV. It designs
 * to allocate all id for data to be written at once, hence it does not need run inside a txn.
 */
public final class RowIDAllocator {
  private long start;
  private long end;
  private final long dbId;
  private long step;

  private RowIDAllocator(long dbId, long step) {
    this.dbId = dbId;
    this.step = step;
  }

  public static RowIDAllocator create(
      long dbId, long tableId,  Catalog catalog, boolean unsigned, long step) {
    RowIDAllocator allocator = new RowIDAllocator(dbId, step);
    if (unsigned) {
      allocator.initUnsigned(catalog, tableId);
    } else {
      allocator.initSigned(catalog, tableId);
    }
    return allocator;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  private void initSigned(Catalog catalog, long tableId) {
    long newEnd;
    if (start == end) {
      // get new start from TiKV, and calculate new end and set it back to TiKV.
      long newStart = catalog.getAutoTableId(dbId, tableId);
      long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
      if (tmpStep != step) {
        throw new TiBatchWriteException("cannot allocate ids for this write");
      }
      newEnd = catalog.getAutoTableId(dbId, tableId, tmpStep);
      if (newStart == Long.MAX_VALUE) {
        throw new TiBatchWriteException("cannot allocate more ids since it ");
      }
      start = newStart;
      end = newEnd;
    }
  }

  private void initUnsigned(Catalog catalog, long tableId) {
    long newEnd;
    if (start == end) {
      // get new start from TiKV, and calculate new end and set it back to TiKV.
      long newStart = catalog.getAutoTableId(dbId, tableId);
      // for unsigned long, -1L is max value.
      long tmpStep = UnsignedLongs.min(-1L - newStart, step);
      if (tmpStep != step) {
        throw new TiBatchWriteException("cannot allocate ids for this write");
      }
      newEnd = catalog.getAutoTableId(dbId, tableId, tmpStep);
      // when compare unsigned long, the min value is largest value.
      if (start == -1L) {
        throw new TiBatchWriteException(
            "cannot allocate more ids since the start reaches " + "unsigned long's max value ");
      }
      end = newEnd;
    }
  }
}
