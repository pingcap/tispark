/*
 * Copyright 2022 PingCAP, Inc.
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

package org.apache.spark.sql.connector.write

import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

case class TiDBWriteBuilder(
    info: LogicalWriteInfo,
    tiDBOptions: TiDBOptions,
    sqlContext: SQLContext)
    extends WriteBuilder {
  override def build(): V1Write =
    new V1Write {
      override def toInsertableRelation: InsertableRelation = {
        new InsertableRelation {
          override def insert(data: DataFrame, overwrite: Boolean): Unit = {
            val schema = info.schema()
            val df = sqlContext.sparkSession.createDataFrame(data.toJavaRDD, schema)
            df.write
              .format("tidb")
              .options(tiDBOptions.parameters)
              .option(TiDBOptions.TIDB_DATABASE, tiDBOptions.database)
              .option(TiDBOptions.TIDB_TABLE, tiDBOptions.table)
              .option(TiDBOptions.TIDB_DEDUPLICATE, "false")
              .mode(SaveMode.Append)
              .save()
          }
        }
      }
    }
}
