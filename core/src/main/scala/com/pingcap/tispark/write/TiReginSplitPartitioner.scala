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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.write

import com.pingcap.tikv.key.Key
import org.apache.spark.Partitioner

class TiReginSplitPartitioner(orderedSplitPoints: List[SerializableKey], maxWriteTaskNumber: Int)
    extends Partitioner {
  override def getPartition(key: Any): Int = {
    val serializableKey = key.asInstanceOf[SerializableKey]
    val rawKey = Key.toRawKey(serializableKey.bytes)
    binarySearch(rawKey) % numPartitions
  }

  private def binarySearch(key: Key): Int = {
    var l = 0
    var r = orderedSplitPoints.size
    while (l < r) {
      val mid = l + (r - l) / 2
      val splitPointKey = orderedSplitPoints(mid).getRowKey
      if (splitPointKey.compareTo(key) < 0) {
        l = mid + 1
      } else {
        r = mid
      }
    }
    l
  }

  override def numPartitions: Int = {
    val partitionNumber = orderedSplitPoints.size + 1
    if (maxWriteTaskNumber > 0) {
      Math.min(maxWriteTaskNumber, partitionNumber)
    } else {
      partitionNumber
    }
  }
}
