/*
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
 */

package com.pingcap.tispark.accumulator

import java.util

import com.pingcap.tikv.event.CacheInvalidateEvent
import org.apache.spark.util.AccumulatorV2

import scala.collection.JavaConversions._

/**
 * A cache invalidate request collector.
 *
 * In common execution of a spark job, executor nodes may receive cache invalidate information
 * and flush executor's own cache store in that node, without explicitly notifying driver node
 * to invalidate cache information. This class is used for accumulating cache invalidate event
 * and make driver node easier to decide when to update it's PD-cache.
 */
class CacheInvalidateAccumulator
    extends AccumulatorV2[CacheInvalidateEvent, Seq[CacheInvalidateEvent]] {
  private final val eventSet: util.Set[CacheInvalidateEvent] =
    new util.HashSet[CacheInvalidateEvent]

  override def isZero: Boolean = eventSet.isEmpty

  override def reset(): Unit = eventSet.clear()

  override def add(v: CacheInvalidateEvent): Unit =
    eventSet.synchronized {
      eventSet.add(v)
    }

  override def copy(): AccumulatorV2[CacheInvalidateEvent, Seq[CacheInvalidateEvent]] = {
    val accumulator = new CacheInvalidateAccumulator
    eventSet.synchronized {
      accumulator.eventSet.addAll(eventSet)
    }
    accumulator
  }

  override def merge(
      other: AccumulatorV2[CacheInvalidateEvent, Seq[CacheInvalidateEvent]]): Unit =
    eventSet.addAll(other.value)

  override def value: Seq[CacheInvalidateEvent] = eventSet.toList

  def remove(event: CacheInvalidateEvent): Boolean =
    eventSet.synchronized {
      eventSet.remove(event)
    }
}
