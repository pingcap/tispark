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
