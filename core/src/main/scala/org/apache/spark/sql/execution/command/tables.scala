/*
 * Copyright 2018 PingCAP, Inc.
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
package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, TiContext}

/**
 * CHECK Spark [[org.apache.spark.sql.execution.command.ShowTablesCommand]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original ShowTablesCommand
 */
case class TiShowTablesCommand(tiContext: TiContext, delegate: ShowTablesCommand)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val db = delegate.databaseName.getOrElse(tiCatalog.getCurrentDatabase)
    // Show the information of tables.
    val tables =
      delegate.tableIdentifierPattern
        .map(tiCatalog.listTables(db, _))
        .getOrElse(tiCatalog.listTables(db))
    tables.map { tableIdent =>
      val database = tableIdent.database.getOrElse("")
      val tableName = tableIdent.table
      val isTemp = tiCatalog.isTemporaryTable(tableIdent)
      if (delegate.isExtended) {
        val information = tiCatalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
        Row(database, tableName, isTemp, s"$information\n")
      } else {
        Row(database, tableName, isTemp)
      }
    }
    Seq.empty[Row]
  }
}

class DescribeTableInfo(val tableName: TableIdentifier,
                        val partitionSpec: TablePartitionSpec,
                        val isExtended: Boolean) {}

/**
 * CHECK Spark [[org.apache.spark.sql.execution.command.ShowColumnsCommand]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original ShowColumnsCommand
 */
case class TiShowColumnsCommand(tiContext: TiContext, delegate: ShowColumnsCommand)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = delegate.databaseName
    val tableName = delegate.tableName
    val catalog = tiCatalog
    val resolver = sparkSession.sessionState.conf.resolver
    val lookupTable = databaseName match {
      case None => tableName
      case Some(db) if tableName.database.exists(!resolver(_, db)) =>
        throw new AnalysisException(
          s"SHOW COLUMNS with conflicting databases: '$db' != '${tableName.database.get}'"
        )
      case Some(db) => TableIdentifier(tableName.identifier, Some(db))
    }
    val table = catalog.getTempViewOrPermanentTableMetadata(lookupTable)
    table.schema.map { c =>
      Row(c.name)
    }

    Seq.empty[Row]
  }
}
