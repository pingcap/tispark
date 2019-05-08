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
package com.pingcap.tikv;

import com.google.common.primitives.UnsignedLongs;
import com.pingcap.tikv.catalog.Catalog;

/**
 * IDAllocator allocates unique value for each row associated with the database id and table id.
 * It first reads from TiKV and calculate the new value with step, then write the new value back to
 * TiKV.
 * The [start, end) range will be lost if job finished or crashed.
 */
public class IDAllocator {
  private long start;
  private long end;
  private final boolean unsigned;
  private final long dbId;
  private Catalog catalog;
  private long step;

  public IDAllocator(long dbId, Catalog catalog, boolean unsigned, long step) {
    this.catalog = catalog;
    this.dbId = dbId;
    this.unsigned = unsigned;
    this.step = step;
  }

  public long getStart() {
    return start;
  }

  public long getEnd() {
    return end;
  }

  public boolean isUnsigned() {
    return unsigned;
  }

  public synchronized long alloc(long tableId) {
    if (isUnsigned()) {
      return allocUnSigned(tableId);
    }
    return allocSigned(tableId);
  }

  // TODO: refine this exception
  private long allocSigned(long tableId) {
    long newEnd;
    if (start == end) {
      // get new start from TiKV, and calculate new end and set it back to TiKV.
      long newStart = allocID(dbId, tableId);
      long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
      if (tmpStep != step) {
        throw new IllegalArgumentException("cannot allocate ids for this write");
      }
      newEnd = allocID(dbId, tableId, tmpStep);
      if (newStart == Long.MAX_VALUE) {
        throw new IllegalArgumentException("cannot allocate more ids since it ");
      }
      start = newStart;
      end = newEnd;
    }

    return start++;
  }

  // TODO: refine this exception
  private long allocUnSigned(long tableId) {
    long newEnd;
    if (start == end) {
      // get new start from TiKV, and calculate new end and set it back to TiKV.
      long newStart = catalog.getAutoTableId(dbId, tableId);
      // for unsigned long, -1L is max value.
      long tmpStep = UnsignedLongs.min(-1L - newStart, step);
      if (tmpStep != step) {
        throw new IllegalArgumentException("cannot allocate ids for this write");
      }
      newEnd = allocID(dbId, tableId, tmpStep);
      // when compare unsigned long, the min value is largest value.
      if (start == -1L) {
        throw new IllegalArgumentException(
            "cannot allocate more ids since the start reaches " + "unsigned long's max value ");
      }
      end = newEnd;
    }

    return start++;
  }

  private long allocID(long dbId, long tableId) {
    return catalog.getAutoTableId(dbId, tableId);
  }

  private long allocID(long dbId, long tableId, long step) {
    return catalog.getAutoTableId(dbId, tableId, step);
  }
}
