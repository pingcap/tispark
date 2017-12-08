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

import java.util.Objects
import java.util.logging.Logger

import com.pingcap.tispark.accumulator.CacheInvalidateAccumulator
import com.pingcap.tispark.handler.CacheInvalidateEventHandler
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart}

class PDCacheInvalidateListener(sparkContext: SparkContext,
                                cacheInvalidateAccumulator: CacheInvalidateAccumulator,
                                handler: CacheInvalidateEventHandler)
    extends SparkListener {
  val logger: Logger = Logger.getLogger(getClass.getName)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    cacheInvalidateAccumulator.reset()
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if (!cacheInvalidateAccumulator.isZero) {
      val eventList = cacheInvalidateAccumulator.value
      logger.warning(
        s"Receiving ${eventList.size} invalidate cache request(s) from job ${jobEnd.jobId} at driver."
      )
      eventList.foreach(handler.handle)
    }
  }
}
