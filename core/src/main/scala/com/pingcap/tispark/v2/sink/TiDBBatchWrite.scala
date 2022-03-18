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
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.v2.sink

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.TiContext
import org.apache.spark.sql.connector.write._

/**
 * Use V1WriteBuilder before turn to v2
 */
case class TiDBBatchWrite(logicalInfo: LogicalWriteInfo, tiDBOptions: TiDBOptions)(
    @transient val tiContext: TiContext)
    extends BatchWrite {
  override def createBatchWriterFactory(info: PhysicalWriteInfo): DataWriterFactory =
    TiDBDataWriterFactory(logicalInfo.schema(), tiDBOptions, tiContext.tiConf)

  override def commit(messages: Array[WriterCommitMessage]): Unit = ???

  override def abort(messages: Array[WriterCommitMessage]): Unit = ???
}
