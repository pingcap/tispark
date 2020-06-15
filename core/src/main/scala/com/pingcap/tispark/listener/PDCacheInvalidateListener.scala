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

import com.pingcap.tispark.accumulator.CacheInvalidateAccumulator
import com.pingcap.tispark.handler.CacheInvalidateEventHandler
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd}

class PDCacheInvalidateListener(
    accumulator: CacheInvalidateAccumulator,
    handler: CacheInvalidateEventHandler)
    extends SparkListener {
  private final val logger: Logger = Logger.getLogger(getClass.getName)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit =
    if (accumulator != null && !accumulator.isZero && handler != null) {
      synchronized {
        if (!accumulator.isZero) {
          val events = accumulator.value
          logger.info(
            s"Receiving ${events.size} cache invalidation request(s) from job ${jobEnd.jobId} at driver. " +
              s"This indicates that there's exception(s) thrown in executor node when communicating with " +
              s"TiKV, checkout executors' log for more information.")
          events.foreach(handler.handle)
        }
      }
    }
}
