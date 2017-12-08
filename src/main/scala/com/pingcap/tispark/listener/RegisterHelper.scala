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

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.event.CacheInvalidateEvent
import com.pingcap.tispark.accumulator.CacheInvalidateAccumulator
import com.pingcap.tispark.handler.CacheInvalidateEventHandler
import org.apache.spark.SparkContext

object RegisterHelper {
  def registerCacheListener(sparkContext: SparkContext, tiSession: TiSession): Unit = {
    val cacheInvalidateAccumulator = new CacheInvalidateAccumulator
    tiSession.injectCallBackFunc(new java.util.function.Function[CacheInvalidateEvent, Void] {
      override def apply(t: CacheInvalidateEvent): Void = {
        // this operation shall be executed in executor nodes
        cacheInvalidateAccumulator.add(t)
        null
      }
    })

    sparkContext.addSparkListener(
      new PDCacheInvalidateListener(
        sparkContext,
        cacheInvalidateAccumulator,
        CacheInvalidateEventHandler(tiSession.getRegionManager)
      )
    )
  }
}
