package com.pingcap.tikv.util;

final public class Oracle {
  private static final long physicalShiftBits = 18;

  public static boolean IsExpired(long lockTS, long ttl) {
    // Because the UNIX time in milliseconds is in long style and will
    // not exceed to become the negative number, so the comparison is correct
    return System.currentTimeMillis() >= ExtractPhysical(lockTS) + ttl;
  }

  private static long ExtractPhysical(long ts) {
    return ts >> physicalShiftBits;
  }
}
