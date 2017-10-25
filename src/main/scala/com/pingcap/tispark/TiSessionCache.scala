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

package com.pingcap.tispark

import java.util.HashMap

import com.pingcap.tikv.{TiConfiguration, TiSession}

object TiSessionCache {
  private val sessionCache: HashMap[(String, Long), TiSession] = new HashMap[(String, Long), TiSession]()

  def getSession(appId: String, execId: Long, conf: TiConfiguration): TiSession = this.synchronized {
    val session = sessionCache.get((appId, execId))
    if (session == null) {
      val newSession = TiSession.create(conf)
      sessionCache.put((appId, execId), newSession)
      newSession
    } else {
      session
    }
  }
}
