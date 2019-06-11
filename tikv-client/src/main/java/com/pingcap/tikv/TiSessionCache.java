package com.pingcap.tikv;

import java.util.HashMap;
import java.util.Map;

public class TiSessionCache {
  private static Map<String, TiSession> sessionCachedMap = new HashMap<>();

  // Since we create session as singleton now, configuration change will not
  // reflect change
  public static synchronized TiSession getSession(TiConfiguration conf) {
    String key = conf.getPdAddrsString();
    if (sessionCachedMap.containsKey(key)) {
      return sessionCachedMap.get(key);
    }

    TiSession newSession = TiSession.create(conf);
    sessionCachedMap.put(key, newSession);
    return newSession;
  }

  public static void clear() {
    sessionCachedMap.clear();
  }
}
