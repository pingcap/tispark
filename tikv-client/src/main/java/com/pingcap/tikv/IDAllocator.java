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

import com.pingcap.tikv.catalog.Catalog;

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

  public boolean isUnsigned() {
    return unsigned;
  }

  public long alloc(long tableId) {
    if (isUnsigned()) {
      return allocUnSigned(tableId);
    }
    return allocSigned(tableId);
  }

  private long allocSigned(long tableId) {
    long newEnd;
    if (start == end) {
      // get new start from TiKV, and calculate new end and set it back to TiKV.
      long newStart = allocID(dbId, tableId);
      long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
      newEnd = allocID(dbId, tableId, tmpStep);
      if (newStart == Long.MAX_VALUE) {
        // TODO: refine this exception
        throw new IllegalArgumentException("cannot allocate more ids since it ");
      }
      start = newStart;
      end = newEnd;
    }

    return start++;
  }

  private long allocUnSigned(long tableId) {
        long newEnd;
        if(start == end) {
          // get new start from TiKV, and calculate new end and set it back to tikv.
          long newStart = catalog.getAutoTableId(dbId, tableId);
          if(newStart < 0) {

          }
          long tmpStep = Math.min(Long.MAX_VALUE - newStart, step);
          newEnd  = allocID(dbId, tableId, tmpStep);
          // when compare unsigned long, the min value is largest value.
          if(start == Long.MIN_VALUE) {
            // TODO: refine this exception
            throw new IllegalArgumentException("cannot allocate more ids since it ");
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
