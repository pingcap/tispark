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

import com.pingcap.tikv.key.Handle
import com.pingcap.tispark.write.TiBatchWrite.TiRow

case class WrappedEncodedRow(
    row: TiRow,
    handle: Handle,
    encodedKey: SerializableKey,
    encodedValue: Array[Byte],
    isIndex: Boolean,
    indexId: Long,
    remove: Boolean)
    extends Ordered[WrappedEncodedRow] {
  override def compare(that: WrappedEncodedRow): Int = this.handle.compare(that.handle)

  override def hashCode(): Int = encodedKey.hashCode()
}
