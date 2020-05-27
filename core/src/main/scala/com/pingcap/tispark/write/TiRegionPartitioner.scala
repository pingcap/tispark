/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tispark.write

import java.util

import com.pingcap.tikv.key.Key
import com.pingcap.tikv.region.TiRegion
import org.apache.spark.Partitioner

class TiRegionPartitioner(regions: util.List[TiRegion], writeConcurrency: Int) extends Partitioner {
  def binarySearch(key: Key): Int = {
    if (regions.get(0).contains(key)) {
      return 0
    }
    var l = 0
    var r = regions.size()
    while (l < r) {
      val mid = l + (r - l) / 2
      val region = regions.get(mid)
      if (Key.toRawKey(region.getEndKey).compareTo(key) <= 0) {
        l = mid + 1
      } else {
        r = mid
      }
    }
    assert(regions.get(l).contains(key))
    l
  }

  override def numPartitions: Int = if (writeConcurrency <= 0) regions.size() else writeConcurrency

  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)

    binarySearch(rawKey) % numPartitions
  }
}
