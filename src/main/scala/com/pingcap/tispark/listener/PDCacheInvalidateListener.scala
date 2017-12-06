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

import java.util.logging.Logger

import com.pingcap.tikv.event.CacheInvalidateEvent
import com.pingcap.tikv.region.RegionManager
import com.pingcap.tispark.accumulator.CacheInvalidateAccumulator
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

class PDCacheInvalidateListener(cacheInvalidateAccumulator: CacheInvalidateAccumulator,
                                regionManager: RegionManager)
    extends SparkListener {
  val logger: Logger = Logger.getLogger(getClass.getName)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    logger.warning("I am running.......")
    cacheInvalidateAccumulator.reset()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    logger.warning("Fuck Job ended......")
    if (!cacheInvalidateAccumulator.isZero) {
      val eventList = cacheInvalidateAccumulator.value
      logger.warning(
        s"Receiving ${eventList.size} invalidate cache request(s) from job ${jobEnd.jobId} at executor."
      )
      eventList.foreach((e: CacheInvalidateEvent) => {
        if (e.shouldUpdateRegion()) {
          logger.warning(s"Invalidating region ${e.getRegionId} cache at executor.")
          regionManager.invalidateRegion(e.getRegionId)
        }

        if (e.shouldUpdateStore()) {
          logger.warning(s"Invalidating store ${e.getStoreId} cache at executor.")
          regionManager.invalidateStore(e.getStoreId)
        }
      })
    }
  }
}
