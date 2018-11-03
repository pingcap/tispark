package com.pingcap.tikv.util;

final public class TsoUtils {
  private static final long physicalShiftBits = 18;

  public static boolean isExpired(long lockTS, long ttl) {
    // Because the UNIX time in milliseconds is in long style and will
    // not exceed to become the negative number, so the comparison is correct
    return System.currentTimeMillis() >= extractPhysical(lockTS) + ttl;
  }

  private static long extractPhysical(long ts) {
    return ts >> physicalShiftBits;
  }
}
