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

import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog, TiSessionCatalog}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, TiContext}

import scala.collection.mutable.ArrayBuffer

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
  }
}

case class TiDescribeTablesCommand(tiContext: TiContext, delegate: DescribeTableCommand)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] =
    tiCatalog
      .catalogOf(delegate.table.database)
      .getOrElse(
        throw new NoSuchDatabaseException(
          delegate.table.database.getOrElse(tiCatalog.getCurrentDatabase)
        )
      ) match {
      case _: TiSessionCatalog =>
        val result = new ArrayBuffer[Row]
        if (delegate.partitionSpec.nonEmpty) {
          throw new AnalysisException(
            s"DESC PARTITION is not allowed on Flash table: ${delegate.table.identifier}"
          )
        }
        val metadata = tiCatalog.getTableMetadata(delegate.table)
        describeSchema(metadata.schema, result, header = false)

        if (delegate.isExtended) {
          describeFormattedTableInfo(metadata, result)
        }

        result
      case _: SessionCatalog => super.run(sparkSession)
    }

  private def describeSchema(schema: StructType,
                             buffer: ArrayBuffer[Row],
                             header: Boolean): Unit = {
    if (header) {
      append(buffer, s"# ${output.head.name}", output(1).name, output(2).name)
    }
    schema.foreach { column =>
      append(buffer, column.name, column.dataType.simpleString, column.getComment().orNull)
    }
  }

  private def append(buffer: ArrayBuffer[Row],
                     column: String,
                     dataType: String,
                     comment: String): Unit =
    buffer += Row(column, dataType, comment)

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // So far we only have engine name and primary key for extended information.
    // TODO: Add more extended table information.
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
  }

}
