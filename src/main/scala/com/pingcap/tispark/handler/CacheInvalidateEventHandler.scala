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
  val regionStoreHandler = new RegionStoreInvalidateHandler
  val leaderHandler = new LeaderUpdateHandler

  def handle(e: CacheInvalidateEvent): Unit = e.getCacheType match {
    case CacheType.REGION_STORE => regionStoreHandler.handle(e)
    case CacheType.LEADER       => leaderHandler.handle(e)
    case _                      => throw new IllegalArgumentException("Unsupported cache invalidate type.")
  }

  class RegionStoreInvalidateHandler extends CacheInvalidateEventHandler(regionManager) {
    val logger: Logger = Logger.getLogger(getClass.getName)

    override def handle(e: CacheInvalidateEvent): Unit = {
      if (e.shouldUpdateRegion()) {
        logger.warning(s"Invalidating region ${e.getRegionId} cache at executor.")
        regionManager.invalidateRegion(e.getRegionId)
      }

      if (e.shouldUpdateStore()) {
        logger.warning(s"Invalidating store ${e.getStoreId} cache at executor.")
        regionManager.invalidateStore(e.getStoreId)
      }
    }
  }

  class LeaderUpdateHandler extends CacheInvalidateEventHandler(regionManager) {
    val logger: Logger = Logger.getLogger(getClass.getName)

    override def handle(e: CacheInvalidateEvent): Unit = {
      logger.warning(
        s"Invalidating leader of region:${e.getRegionId} store:${e.getStoreId} cache at executor."
      )
      regionManager.updateLeader(e.getRegionId, e.getStoreId)
    }
  }

}

object CacheInvalidateEventHandler {
  def apply(regionManager: RegionManager): CacheInvalidateEventHandler =
    new CacheInvalidateEventHandler(regionManager)
}
