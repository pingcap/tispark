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

package com.pingcap.tispark.handler

import com.pingcap.tikv.event.CacheInvalidateEvent
import com.pingcap.tikv.event.CacheInvalidateEvent.CacheType
import com.pingcap.tikv.region.RegionManager
import com.pingcap.tispark.listener.CacheInvalidateListener
import org.slf4j.LoggerFactory

/**
 * A CacheInvalidateEventHandler as it's name indicates what this class will do.
 *
 * Since there's only one event and one event handler currently in the project, we
 * don't need to over-design our event handler to support millions of other events.
 *
 * Refactor this if we need to support tons of events.
 *
 * @param regionManager Region manager used for sending invalidating cache. Usually
 *                      it's Spark driver's regionManager
 */
class CacheInvalidateEventHandler(regionManager: RegionManager) {
  private final val logger = LoggerFactory.getLogger(getClass.getName)

  def handle(event: CacheInvalidateEvent): Unit = {
    try {
      event.getCacheType match {
        case CacheType.REGION_STORE =>
          // Used for updating region/store cache in the given regionManager
          if (event.shouldUpdateRegion()) {
            logger.info(s"Invalidating region ${event.getRegionId} cache at driver.")
            regionManager.invalidateRegion(event.getRegionId)
          }

          if (event.shouldUpdateStore()) {
            logger.info(s"Invalidating store ${event.getStoreId} cache at driver.")
            regionManager.invalidateStore(event.getStoreId)
          }
        case CacheType.LEADER =>
          // Used for updating leader information cached in the given regionManager
          logger.info(
            s"Invalidating leader of region:${event.getRegionId} store:${event.getStoreId} cache at driver.")
          regionManager.updateLeader(event.getRegionId, event.getStoreId)
        case CacheType.REQ_FAILED =>
          logger.info(s"Request failed cache invalidation for region ${event.getRegionId}")
          regionManager.onRequestFail(event.getRegionId, event.getStoreId)
        case _ => throw new IllegalArgumentException("Unsupported cache invalidate type.")
      }
    } catch {
      case e: Exception =>
        logger.error(s"Updating cache failed:${e.getMessage}")
        return
    }
    CacheInvalidateListener.getInstance().CACHE_INVALIDATE_ACCUMULATOR.remove(event)
  }
}

object CacheInvalidateEventHandler {
  def apply(regionManager: RegionManager): CacheInvalidateEventHandler =
    new CacheInvalidateEventHandler(regionManager)
}
