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

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, TiContext}

/**
 * TiDB Source implementation for Spark SQL
 */
class DefaultSource
    extends DataSourceRegister
    with RelationProvider
    with SchemaRelationProvider
    with CreatableRelationProvider {

  override def shortName(): String = "tidb"

  /**
   * Create a new `TiDBRelation` instance using parameters from Spark SQL DDL.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    TiSparkConnectorUtils.checkVersionAndEnablePushdown(sqlContext.sparkSession)

    val options = new TiDBOptions(parameters)
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))

    val tableRef = TiDBUtils.getTableRef(options.dbtable, tiContext.tiCatalog.getCurrentDatabase)
    TiDBRelation(tiContext.tiSession, tableRef, tiContext.meta)(sqlContext)
  }

  /**
   * Load a `TiDBRelation` using user-provided schema, so no inference over TiDB will be used.
   */
  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String],
                              schema: StructType): BaseRelation =
    // TODO: use schema info
    createRelation(sqlContext, parameters)

  /**
   * Creates a Relation instance by first writing the contents of the given DataFrame to TiDB
   */
  override def createRelation(sqlContext: SQLContext,
                              saveMode: SaveMode,
                              parameters: Map[String, String],
                              df: DataFrame): BaseRelation = {
    TiSparkConnectorUtils.checkVersionAndEnablePushdown(sqlContext.sparkSession)

    val options = new TiDBOptions(parameters)
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))
    val conn = TiDBUtils.createConnectionFactory(options)()

    try {
      val tableExists = TiDBUtils.tableExists(conn, options)
      if (tableExists) {
        saveMode match {
          case SaveMode.Overwrite =>
            if (options.isTruncate && TiDBUtils
                  .isCascadingTruncateTable(options.url)
                  .contains(false)) {
              // In this case, we should truncate table and then load.
              TiDBUtils.truncateTable(conn, options)
              val tableSchema = TiDBUtils.getSchemaOption(conn, options)
              TiDBUtils.saveTable(tiContext, df, tableSchema, options)
            } else {
              // Otherwise, do not truncate the table, instead drop and recreate it
              TiDBUtils.dropTable(conn, options)
              TiDBUtils.createTable(conn, df, options)
              TiDBUtils.saveTable(tiContext, df, Some(df.schema), options)
            }

          case SaveMode.Append =>
            val tableSchema = TiDBUtils.getSchemaOption(conn, options)
            TiDBUtils.saveTable(tiContext, df, tableSchema, options)

          case SaveMode.ErrorIfExists =>
            throw new Exception(
              s"Table or view '${options.dbtable}' already exists. SaveMode: ErrorIfExists."
            )

          case SaveMode.Ignore =>
          // With `SaveMode.Ignore` mode, if table already exists, the save operation is expected
          // to not save the contents of the DataFrame and to not change the existing data.
          // Therefore, it is okay to do nothing here and then just return the relation below.
        }
      } else {
        TiDBUtils.createTable(conn, df, options)
        TiDBUtils.saveTable(tiContext, df, Some(df.schema), options)
      }
    } finally {
      conn.close()
    }

    createRelation(sqlContext, parameters)
  }
}
