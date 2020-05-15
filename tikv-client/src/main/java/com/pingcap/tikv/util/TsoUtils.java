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
