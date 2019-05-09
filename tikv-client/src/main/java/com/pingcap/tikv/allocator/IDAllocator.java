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
import java.io.Serializable;

/**
 * IDAllocator allocates unique value for each row associated with the database id and table id. It
 * first reads from TiKV and calculate the new value with step, then write the new value back to
 * TiKV. The [start, end) range will be lost if job finished or crashed.
 */
public class IDAllocator implements Serializable {
  private long start;
  private long end;
  private final long dbId;
  private long step;

  private IDAllocator(long dbId, long step) {
    this.dbId = dbId;
    this.step = step;
  }

  public static IDAllocator create(
      long dbId, long tableId, Catalog catalog, boolean unsigned, long step) {
    IDAllocator allocator = new IDAllocator(dbId, step);
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

  public synchronized long alloc() throws IllegalAccessException {
    if (start == end) {
      throw new IllegalAccessException("cannot allocate any more");
    }
    return ++start;
  }

  private void initSigned(Catalog catalog, long tableId) {
    long newEnd;
    if (start == end) {
      // get new start from TiKV, and calculate new end and set it back to TiKV.
      long newStart = catalog.getAutoTableId(dbId, tableId);
      long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
      if (tmpStep != step) {
        throw new IllegalArgumentException("cannot allocate ids for this write");
      }
      newEnd = catalog.getAutoTableId(dbId, tableId, tmpStep);
      if (newStart == Long.MAX_VALUE) {
        throw new IllegalArgumentException("cannot allocate more ids since it ");
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
        throw new IllegalArgumentException("cannot allocate ids for this write");
      }
      newEnd = catalog.getAutoTableId(dbId, tableId, tmpStep);
      // when compare unsigned long, the min value is largest value.
      if (start == -1L) {
        throw new IllegalArgumentException(
            "cannot allocate more ids since the start reaches " + "unsigned long's max value ");
      }
      end = newEnd;
    }
  }
}
