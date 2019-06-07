package com.pingcap.tikv;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TiSessionCache {
  private static Map<String, TiSession> sessionCachedMap = new ConcurrentHashMap<>();

  // Since we create session as singleton now, configuration change will not
  // reflect change
  public static TiSession getSession(TiConfiguration conf) {
    return sessionCachedMap.putIfAbsent(conf.getPdAddrsString(), TiSession.create(conf));
  }
}
