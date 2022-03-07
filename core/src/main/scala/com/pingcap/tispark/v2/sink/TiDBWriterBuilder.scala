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

import com.pingcap.tispark.write.{TiDBOptions, TiDBWriter}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1WriteBuilder}
import org.apache.spark.sql.sources.InsertableRelation

case class TiDBWriterBuilder(
    info: LogicalWriteInfo,
    tiDBOptions: TiDBOptions,
    sqlContext: SQLContext)
    extends V1WriteBuilder {

  // TODO rewrite write and extend WriteBuilder rather than V1WriteBuilder
  override def buildForV1Write(): InsertableRelation = { (data: DataFrame, overwrite: Boolean) =>
    {
      val saveMode = if (overwrite) {
        SaveMode.Overwrite
      } else {
        SaveMode.Append
      }
      TiDBWriter.write(data, sqlContext, saveMode, tiDBOptions)
    }
  }

}
