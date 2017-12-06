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

import java.util.logging.Logger

import com.pingcap.tikv.event.CacheInvalidateEvent
import com.pingcap.tikv.event.CacheInvalidateEvent.CacheType
import com.pingcap.tikv.region.RegionManager

class CacheInvalidateEventHandler(regionManager: RegionManager) {
  private final val logger = Logger.getLogger(getClass.getName)

  def handle(e: CacheInvalidateEvent): Unit = e.getCacheType match {
    case CacheType.REGION_STORE =>
      // Used for updating region/store cache in the given regionManager
      if (e.shouldUpdateRegion()) {
        logger.warning(s"Invalidating region ${e.getRegionId} cache at driver.")
        regionManager.invalidateRegion(e.getRegionId)
      }

      if (e.shouldUpdateStore()) {
        logger.warning(s"Invalidating store ${e.getStoreId} cache at driver.")
        regionManager.invalidateStore(e.getStoreId)
      }
    case CacheType.LEADER =>
      // Used for updating leader information cached in the given regionManager
      logger.warning(
        s"Invalidating leader of region:${e.getRegionId} store:${e.getStoreId} cache at driver."
      )
      regionManager.updateLeader(e.getRegionId, e.getStoreId)
    case _ => throw new IllegalArgumentException("Unsupported cache invalidate type.")
  }
}

object CacheInvalidateEventHandler {
  def apply(regionManager: RegionManager): CacheInvalidateEventHandler =
    new CacheInvalidateEventHandler(regionManager)
}
