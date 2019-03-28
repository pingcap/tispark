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
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog, TiSessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, TiContext}

import scala.collection.mutable.ArrayBuffer

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
  }
}

/**
 * CHECK Spark [[org.apache.spark.sql.execution.command.DescribeTableCommand]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original DescribeTableCommand
 */
case class TiDescribeTablesCommand(tiContext: TiContext, delegate: DescribeTableCommand)
    extends TiCommand(delegate) {
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference(
      "col_name",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build()
    )(),
    AttributeReference(
      "data_type",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build()
    )(),
    AttributeReference(
      "nullable",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "whether the column is nullable").build()
    )(),
    AttributeReference(
      "comment",
      StringType,
      nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build()
    )()
  )

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
            s"DESC PARTITION is not supported on TiDB table: ${delegate.table.identifier}"
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
      append(
        buffer,
        column.name,
        column.dataType.simpleString,
        column.getComment().orNull,
        column.nullable.toString
      )
    }
  }

  private def append(buffer: ArrayBuffer[Row],
                     column: String,
                     dataType: String,
                     comment: String,
                     nullable: String = ""): Unit =
    buffer += Row(column, dataType, nullable, comment)

  private def describeFormattedTableInfo(table: CatalogTable, buffer: ArrayBuffer[Row]): Unit = {
    // TODO: Add more extended table information.
    append(buffer, "", "", "")
    append(buffer, "# Detailed Table Information", "", "")
    table.toLinkedHashMap.foreach { s =>
      append(buffer, s._1, s._2, "")
    }
  }

}

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
  }
}
