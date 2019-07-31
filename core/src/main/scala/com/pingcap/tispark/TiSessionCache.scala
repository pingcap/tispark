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

import com.pingcap.tikv.{TiConfiguration, TiSession}

import scala.collection.mutable

object TiSessionCache {
  private val sessionCachedMap = mutable.Map[String, TiSession]()

  // Since we create session as singleton now, configuration change will not
  // reflect change
  def getSession(conf: TiConfiguration): TiSession = this.synchronized {
    sessionCachedMap.getOrElseUpdate(conf.getPdAddrsString(), TiSession.create(conf))
  }
}