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
