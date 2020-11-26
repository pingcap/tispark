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

import com.pingcap.tikv.codec.KeyUtils
import com.pingcap.tikv.key.Key
import com.pingcap.tikv.util.{FastByteComparisons, LogDesensitization}

class SerializableKey(val bytes: Array[Byte])
    extends Comparable[SerializableKey]
    with Serializable {
  override def toString: String = LogDesensitization.hide(KeyUtils.formatBytes(bytes))

  override def equals(that: Any): Boolean =
    that match {
      case that: SerializableKey => this.bytes.sameElements(that.bytes)
      case _ => false
    }

  override def hashCode(): Int =
    util.Arrays.hashCode(bytes)

  override def compareTo(o: SerializableKey): Int = {
    FastByteComparisons.compareTo(bytes, o.bytes)
  }

  def getRowKey: Key = {
    Key.toRawKey(bytes)
  }
}
