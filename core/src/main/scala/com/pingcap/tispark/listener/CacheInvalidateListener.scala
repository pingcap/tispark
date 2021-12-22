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
import org.apache.spark.SparkContext
import org.slf4j.LoggerFactory

class CacheInvalidateListener()
    extends Serializable
    with java.util.function.Function[CacheInvalidateEvent, Void] {
  final val CACHE_ACCUMULATOR_NAME = "CacheInvalidateAccumulator"
  final val CACHE_INVALIDATE_ACCUMULATOR = new CacheInvalidateAccumulator

  override def apply(t: CacheInvalidateEvent): Void = {
    // this operation shall be executed in executor nodes
    CACHE_INVALIDATE_ACCUMULATOR.add(t)
    null
  }
}

object CacheInvalidateListener {
  private final val logger = LoggerFactory.getLogger(getClass.getName)
  private var manager: CacheInvalidateListener = _

  def getInstance(): CacheInvalidateListener = {
    if (manager == null) {
      throw new RuntimeException("CacheListenerManager has not been initialized properly.")
    }
    manager
  }

  /**
   * Initialize cache invalidation frame work for the given session.
   *
   * @param sc            The spark SparkContext used for attaching a cache listener.
   * @param regionManager The RegionManager to invalidate local cache.
   */
  def initCacheListener(sc: SparkContext, regionManager: RegionManager): Unit =
    if (manager == null) {
      synchronized {
        if (manager == null) {
          try {
            manager = new CacheInvalidateListener()
            init(sc, regionManager, manager)
          } catch {
            case e: Throwable => logger.error(s"Init CacheListener failed.", e)
          }
        }
      }
    }

  def init(
      sc: SparkContext,
      regionManager: RegionManager,
      manager: CacheInvalidateListener): Unit =
    if (sc != null && regionManager != null) {
      sc.register(manager.CACHE_INVALIDATE_ACCUMULATOR, manager.CACHE_ACCUMULATOR_NAME)
      sc.addSparkListener(
        new PDCacheInvalidateListener(
          manager.CACHE_INVALIDATE_ACCUMULATOR,
          CacheInvalidateEventHandler(regionManager)))
    }
}
