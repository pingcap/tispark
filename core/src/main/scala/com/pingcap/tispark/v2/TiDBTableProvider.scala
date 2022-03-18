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

package com.pingcap.tispark.v2

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.TiDBRelation
import com.pingcap.tispark.write.{TiDBOptions, TiDBWriter}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession, TiExtensions}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import java.util
import scala.collection.JavaConverters._

class TiDBTableProvider
    extends TableProvider
    with DataSourceRegister
    with CreatableRelationProvider {
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
        val tiTableRef = mergeOptions.getTiTableRef(tiContext.tiConf)
        val table = tiContext.meta
          .getTable(tiTableRef.databaseName, tiTableRef.tableName)
          .getOrElse(
            throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName))
        TiDBTable(tiContext.tiSession, tiTableRef, table, ts, Some(mergeOptions))(
          sparkSession.sqlContext)
      case None => throw new TiBatchWriteException("TiExtensions is disable!")
    }
  }

  override def shortName(): String = "tidb"

  // TODO: replace v1 path in next pr
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    val options = new TiDBOptions(parameters)
    TiDBWriter.write(data, sqlContext, mode, options)
    createRelation(sqlContext, parameters)
  }

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {

    val options = new TiDBOptions(parameters)
    val sparkSession = sqlContext.sparkSession

    TiExtensions.getTiContext(sparkSession) match {
      case Some(tiContext) =>
        val ts = tiContext.tiSession.getTimestamp
        TiDBRelation(
          tiContext.tiSession,
          options.getTiTableRef(tiContext.tiConf),
          tiContext.meta,
          ts,
          Some(options))(sqlContext)
      case None => throw new TiBatchWriteException("TiExtensions is disable!")
    }
  }
}
