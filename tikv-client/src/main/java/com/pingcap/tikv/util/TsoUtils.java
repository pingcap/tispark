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

import com.pingcap.tikv.meta.TiTimestamp;

public final class TsoUtils {
  public static boolean isExpired(long lockTS, long ttl) {
    // Because the UNIX time in milliseconds is in long style and will
    // not exceed to become the negative number, so the comparison is correct
    return untilExpired(lockTS, ttl) <= 0;
  }

  public static long untilExpired(long lockTS, long ttl) {
    return TiTimestamp.extractPhysical(lockTS) + ttl - System.currentTimeMillis();
  }
}
