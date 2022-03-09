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

package com.pingcap.tispark.v2

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.{SparkSession, TiExtensions}
import org.apache.spark.sql.connector.catalog.{Identifier, SupportsCatalogOptions, Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class TiDBTableProvider extends SupportsCatalogOptions with DataSourceRegister {
  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    getTable(null, Array.empty[Transform], options.asCaseSensitiveMap()).schema()
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {

    val scalaMap = properties.asScala.toMap
    val mergeOptions = new TiDBOptions(scalaMap)
    val sparkSession = SparkSession.active

    TiExtensions.getTiContext(sparkSession) match {
      case Some(tiContext) =>
        val ts = tiContext.tiSession.getTimestamp
        TiDBTable(
          tiContext.tiSession,
          mergeOptions.getTiTableRef(tiContext.tiConf),
          tiContext.meta,
          ts,
          Some(mergeOptions))(sparkSession.sqlContext)
      case None => throw new TiBatchWriteException("TiExtensions is disable!")
    }
  }

  override def shortName(): String = "tidb"

  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    require(options.get("database") != null, "Option 'database' is required.")
    require(options.get("table") != null, "Option 'table' is required.")
    Identifier.of(Array(options.get("database")), options.get("table"))
  }

  override def extractCatalog(options: CaseInsensitiveStringMap): String = {
    "tidb_catalog"
  }
}
