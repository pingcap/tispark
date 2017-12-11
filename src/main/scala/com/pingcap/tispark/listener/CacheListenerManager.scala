/*
 *
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
 *
 */

package com.pingcap.tispark.listener

import com.pingcap.tikv.event.CacheInvalidateEvent
import com.pingcap.tikv.region.RegionManager
import com.pingcap.tispark.accumulator.CacheInvalidateAccumulator
import com.pingcap.tispark.handler.CacheInvalidateEventHandler
import com.pingcap.tispark.listener.CacheListenerManager._
import org.apache.log4j.Logger
import org.apache.spark.SparkContext

/**
 * Initialize cache invalidation frame work for the given session.
 *
 * @param sc            The spark SparkContext used for attaching a cache listener.
 * @param regionManager The RegionManager to invalidate local cache.
 */
private class CacheListenerManager(sc: SparkContext, regionManager: RegionManager) {
  def init(): Unit = {
    if (sc != null && regionManager != null) {
      sc.register(CACHE_INVALIDATE_ACCUMULATOR, CACHE_ACCUMULATOR_NAME)
      sc.addSparkListener(
        new PDCacheInvalidateListener(
          CACHE_INVALIDATE_ACCUMULATOR,
          CacheInvalidateEventHandler(
            regionManager,
            (e) => CACHE_INVALIDATE_ACCUMULATOR.remove(e),
            null
          )
        )
      )
    }
  }
}

object CacheListenerManager {
  private var manager: CacheListenerManager = _
  private final val logger = Logger.getLogger(getClass.getName)
  final val CACHE_ACCUMULATOR_NAME = "CacheInvalidateAccumulator"
  final val CACHE_INVALIDATE_ACCUMULATOR = new CacheInvalidateAccumulator
  final var CACHE_ACCUMULATOR_FUNCTION =
    new java.util.function.Function[CacheInvalidateEvent, Void] {
      override def apply(t: CacheInvalidateEvent): Void = {
        // this operation shall be executed in executor nodes
        CACHE_INVALIDATE_ACCUMULATOR.add(t)
        null
      }
    }

  def initCacheListener(sc: SparkContext, regionManager: RegionManager): Unit = {
    if (manager == null) {
      synchronized {
        if (manager == null) {
          try {
            manager = new CacheListenerManager(sc, regionManager)
            manager.init()
          } catch {
            case e: Throwable => logger.trace(s"Init CacheListener failed:${e.getMessage}")
          }
        }
      }
    }
  }
}
