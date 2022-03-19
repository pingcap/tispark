/*
 * Copyright 2021 PingCAP, Inc.
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

package com.pingcap.tispark.v2.sink

import com.pingcap.tikv.TiConfiguration
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

/**
 * Use V1WriteBuilder before turn to v2
 */
case class TiDBDataWrite(
    partitionId: Int,
    taskId: Long,
    schema: StructType,
    tiDBOptions: TiDBOptions,
    ticonf: TiConfiguration)
    extends DataWriter[InternalRow] {

  override def write(record: InternalRow): Unit = {
    val row = Row.fromSeq(record.toSeq(schema))
    ???
  }

  override def commit(): WriterCommitMessage = ???

  override def abort(): Unit = {}

  override def close(): Unit = {}
}

object WriteSucceeded extends WriterCommitMessage
