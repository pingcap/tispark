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

import com.pingcap.tikv.event.CacheInvalidateEvent

object AccumulatorManager {
  final val ACCUMULATOR_NAME = "CacheInvalidateAccumulator"
  final val CACHE_INVALIDATE_ACCUMULATOR = new CacheInvalidateAccumulator
  final val CACHE_ACCUMULATOR_FUNCTION =
    new java.util.function.Function[CacheInvalidateEvent, Void] {
      override def apply(t: CacheInvalidateEvent): Void = {
        // this operation shall be executed in executor nodes
        CACHE_INVALIDATE_ACCUMULATOR.add(t)
        null
      }
    }
}
