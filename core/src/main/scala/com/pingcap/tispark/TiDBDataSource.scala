/*
 * Copyright 2019 PingCAP, Inc.
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

import com.pingcap.tikv.exception.TiBatchWriteException
import com.pingcap.tispark.write.{TiDBOptions, TiDBWriter}
import org.apache.spark.sql._
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType

/**
 * TiDB Source implementation for Spark SQL
 */
class TiDBDataSource
    extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  override def shortName(): String = "tidb"

  override def toString: String = "TIDB"

  /**
   * Load a `TiDBRelation` using user-provided schema, so no inference over TiDB will be used.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation =
    // TODO: use schema info
    // pending: https://internal.pingcap.net/jira/browse/TISPARK-98
    createRelation(sqlContext, parameters)

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to TiDB
   */
  override def createRelation(
      sqlContext: SQLContext,
      saveMode: SaveMode,
      parameters: Map[String, String],
      df: DataFrame): BaseRelation = {
    val options = new TiDBOptions(parameters)
    TiDBWriter.write(df, sqlContext, saveMode, options)
    createRelation(sqlContext, parameters)
  }

  /**
   * Create a new `TiDBRelation` instance using parameters from Spark SQL DDL.
   */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {

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
