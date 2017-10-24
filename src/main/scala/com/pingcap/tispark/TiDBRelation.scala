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

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiClientInternalException
import com.pingcap.tikv.meta.{TiSelectRequest, TiTableInfo, TiTimestamp}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType

class TiDBRelation(session: TiSession,
                   tableRef: TiTableReference,
                   meta: MetaManager)
                  (@transient val sqlContext: SQLContext) extends BaseRelation {
  val table: TiTableInfo = meta.getTable(tableRef.databaseName, tableRef.tableName)
                               .getOrElse(throw new TiClientInternalException("Table not exist"))

  override lazy val schema: StructType = TiUtils.getSchemaFromTable(table)

  def logicalPlanToRDD(selectRequest: TiSelectRequest): TiRDD = {
    val ts: TiTimestamp = session.getTimestamp
    selectRequest.setStartTs(ts.getVersion)

    new TiRDD(selectRequest,
              session.getConf,
              tableRef,
              ts,
              sqlContext.sparkSession)
  }
}
