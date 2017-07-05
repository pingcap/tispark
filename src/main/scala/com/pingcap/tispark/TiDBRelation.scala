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

package com.pingcap.tispark

import com.pingcap.tikv.Snapshot
import com.pingcap.tikv.exception.TiClientInternalException
import com.pingcap.tikv.meta.{TiSelectRequest, TiTableInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}

class TiDBRelation(options: TiOptions, meta: MetaManager)(@transient val sqlContext: SQLContext)
  extends BaseRelation {
  val table: TiTableInfo = meta.getTable(options.databaseName, options.tableName)
                               .getOrElse(throw new TiClientInternalException("Table not exist"))

  lazy val snapshot: Snapshot = meta.cluster.createSnapshot()

  override def schema: StructType = {
    val fields = new Array[StructField](table.getColumns.size())
    for (i <- 0 until table.getColumns.size()) {
      val col = table.getColumns.get(i)
      val metadata = new MetadataBuilder()
            .putString("name", col.getName)
            .build()
      fields(i) = StructField(col.getName, TiUtils.toSparkDataType(col.getType), nullable = true, metadata)
    }
    new StructType(fields)
  }

  def logicalPlanToRDD(selectRequest: TiSelectRequest): RDD[Row] = {
    selectRequest.setStartTs(snapshot.getVersion)


    new TiRDD(selectRequest,
              sqlContext.sparkContext,
              options,
              table)
  }
}
