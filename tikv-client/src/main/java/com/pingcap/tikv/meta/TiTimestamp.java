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

package com.pingcap.tikv.meta;

import java.io.Serializable;

/** TiTimestamp is the timestamp returned by timestamp oracle inside placement driver */
public class TiTimestamp implements Serializable {
  private static final int PHYSICAL_SHIFT_BITS = 18;

  private final long physical;
  private final long logical;

  public TiTimestamp(long p, long l) {
    this.physical = p;
    this.logical = l;
  }

  public long getVersion() {
    return (physical << PHYSICAL_SHIFT_BITS) + logical;
  }

  public long getPhysical() {
    return this.physical;
  }

  public long getLogical() {
    return this.logical;
  }
}
