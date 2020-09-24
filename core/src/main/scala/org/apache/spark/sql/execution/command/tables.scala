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
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructType}
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, TiContext}

import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.plans.logical.{Command, Histogram}

/**
 * CHECK Spark [[org.apache.spark.sql.execution.command.ShowTablesCommand]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original ShowTablesCommand
 */
case class TiShowTablesCommand(tiContext: TiContext, delegate: ShowTablesCommand)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val db = delegate.databaseName match {
      case Some("default") => tiCatalog.getCurrentDatabase
      case Some(d) => d
      case None => tiCatalog.getCurrentDatabase
    }

    // Show the information of tables.
    val tables = delegate.tableIdentifierPattern
      .map(tiCatalog.listTables(db, _))
      .getOrElse(tiCatalog.listTables(db, "*"))
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

case class DescribeTableInfo(
    tableName: TableIdentifier,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean) {}

/**
 * CHECK Spark [[org.apache.spark.sql.execution.command.DescribeTableCommand]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original DescribeTableCommand
 */
case class TiDescribeTablesCommand(
    tiContext: TiContext,
    delegate: Command,
    tableInfo: DescribeTableInfo)
    extends TiCommand(delegate) {
  override val output: Seq[Attribute] = Seq(
    // Column names are based on Hive.
    AttributeReference(
      "col_name",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference(
      "data_type",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build())(),
    AttributeReference(
      "nullable",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "whether the column is nullable").build())(),
    AttributeReference(
      "comment",
      StringType,
      nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build())())

  override def run(sparkSession: SparkSession): Seq[Row] = {
    if (tiCatalog.isTemporaryTable(tableInfo.tableName)) {
      val result = new ArrayBuffer[Row]
      val metadata = tiCatalog.getTempViewOrPermanentTableMetadata(tableInfo.tableName)
      describeSchema(metadata.schema, result, header = false)

      if (tableInfo.isExtended) {
        describeFormattedTableInfo(metadata, result)
      }

      result
    } else {
      tiCatalog
        .catalogOf(tableInfo.tableName.database)
        .getOrElse(throw new NoSuchDatabaseException(
          tableInfo.tableName.database.getOrElse(tiCatalog.getCurrentDatabase))) match {
        case tiSessionCatalog: TiSessionCatalog =>
          val result = new ArrayBuffer[Row]
          if (tableInfo.partitionSpec.nonEmpty) {
            throw new AnalysisException(
              s"DESC PARTITION is not supported on TiDB table: ${tableInfo.tableName}")
          }
          val metadata = tiCatalog.getTableMetadata(tableInfo.tableName)
          describeSchema(metadata.schema, result, header = false)

          if (tableInfo.isExtended) {
            describeFormattedTableInfo(metadata, result)
          }

          result
        case _: SessionCatalog =>
          delegate match {
            case dt: DescribeTableCommand =>
              val schema = tiCatalog.getTableMetadata(tableInfo.tableName).schema
              val (delegateResult, extendedResult) =
                dt.run(sparkSession).zipWithIndex.splitAt(schema.length)
              delegateResult.map(
                (r: (Row, Int)) =>
                  Row(
                    r._1(0),
                    r._1(1),
                    schema.fields(r._2).nullable.toString,
                    r._1(2))) ++ extendedResult.map(r => Row(r._1(0), r._1(1), "", r._1(2)))
            case _ =>
              val result = new ArrayBuffer[Row]
              val metadata = tiCatalog.getTableMetadata(tableInfo.tableName)
              describeSchema(metadata.schema, result, header = false)

              if (tableInfo.isExtended) {
                describeFormattedTableInfo(metadata, result)
              }

              result
          }
      }
    }
  }

  private def describeSchema(
      schema: StructType,
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
        column.nullable.toString)
    }
  }

  private def append(
      buffer: ArrayBuffer[Row],
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
 * CHECK Spark [[org.apache.spark.sql.execution.command.DescribeColumnCommand]]
 *
 * @param tiContext tiContext which contains our catalog info
 * @param delegate original DescribeColumnCommand
 */
case class TiDescribeColumnCommand(tiContext: TiContext, delegate: DescribeColumnCommand)
    extends TiCommand(delegate) {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    try {
      doRun(sparkSession, delegate.table)
    } catch {
      case _: Throwable =>
        doRun(
          sparkSession,
          TableIdentifier(delegate.table.table, Some(tiCatalog.getCurrentDatabase)))
    }
  }

  private def doRun(sparkSession: SparkSession, tableWithDBName: TableIdentifier): Seq[Row] = {
    val resolver = sparkSession.sessionState.conf.resolver

    val relation = sparkSession.table(tableWithDBName).queryExecution.analyzed

    val colName = UnresolvedAttribute(delegate.colNameParts).name
    val field = {
      relation.resolve(delegate.colNameParts, resolver).getOrElse {
        throw new AnalysisException(s"Column $colName does not exist")
      }
    }
    if (!field.isInstanceOf[Attribute]) {
      // If the field is not an attribute after `resolve`, then it's a nested field.
      throw new AnalysisException(
        s"DESC TABLE COLUMN command does not support nested data types: $colName")
    }

    val catalogTable = tiCatalog.getTempViewOrPermanentTableMetadata(tableWithDBName)
    val colStats = catalogTable.stats.map(_.colStats).getOrElse(Map.empty)
    val cs = colStats.get(field.name)

    val comment = if (field.metadata.contains("comment")) {
      Option(field.metadata.getString("comment"))
    } else {
      None
    }

    val buffer = ArrayBuffer[Row](
      Row("col_name", field.name),
      Row("data_type", field.dataType.catalogString),
      Row("comment", comment.getOrElse("NULL")))
    if (delegate.isExtended) {
      // Show column stats when EXTENDED or FORMATTED is specified.
      buffer += Row("min", cs.flatMap(_.min.map(_.toString)).getOrElse("NULL"))
      buffer += Row("max", cs.flatMap(_.max.map(_.toString)).getOrElse("NULL"))
      buffer += Row("num_nulls", cs.fold("NULL")(_.nullCount.toString))
      buffer += Row("distinct_count", cs.fold("NULL")(_.distinctCount.toString))
      buffer += Row("avg_col_len", cs.fold("NULL")(_.avgLen.toString))
      buffer += Row("max_col_len", cs.fold("NULL")(_.maxLen.toString))
      val histDesc = for {
        c <- cs
        hist <- c.histogram
      } yield histogramDescription(hist)
      buffer ++= histDesc.getOrElse(Seq(Row("histogram", "NULL")))
    }
    buffer
  }

  private def histogramDescription(histogram: Histogram): Seq[Row] = {
    val header =
      Row("histogram", s"height: ${histogram.height}, num_of_bins: ${histogram.bins.length}")
    val bins = histogram.bins.zipWithIndex.map {
      case (bin, index) =>
        Row(
          s"bin_$index",
          s"lower_bound: ${bin.lo}, upper_bound: ${bin.hi}, distinct_count: ${bin.ndv}")
    }
    header +: bins
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
    val tableName = delegate.tableName.table
    try {
      doRun(tableName, delegate.tableName.database)
    } catch {
      case _: Throwable =>
        doRun(tableName, Some(tiCatalog.getCurrentDatabase))
    }
  }

  private def doRun(tableName: String, databaseName: Option[String]): Seq[Row] = {
    val lookupTable = TableIdentifier(tableName, databaseName)
    val table = tiCatalog.getTempViewOrPermanentTableMetadata(lookupTable)
    table.schema.map { c =>
      Row(c.name)
    }
  }
}

case class TiCreateTableLikeCommand(tiContext: TiContext, delegate: CreateTableLikeCommand)
    extends RunnableCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val catalog = tiContext.tiCatalog
    val sourceTableDesc = catalog.getTempViewOrPermanentTableMetadata(delegate.sourceTable)

    val newProvider = if (sourceTableDesc.tableType == CatalogTableType.VIEW) {
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else if (catalog
        .catalogOf(sourceTableDesc.identifier.database)
        .exists(_.isInstanceOf[TiSessionCatalog])) {
      // TODO: use tikv datasource
      Some(sparkSession.sessionState.conf.defaultDataSourceName)
    } else {
      sourceTableDesc.provider
    }

    // If the location is specified, we create an external table internally.
    // Otherwise create a managed table.
    val tblType =
      if (delegate.fileFormat.locationUri.isEmpty) CatalogTableType.MANAGED
      else CatalogTableType.EXTERNAL

    val newTableDesc =
      CatalogTable(
        identifier = delegate.targetTable,
        tableType = tblType,
        storage = sourceTableDesc.storage
          .copy(locationUri = delegate.fileFormat.locationUri),
        schema = sourceTableDesc.schema,
        provider = newProvider,
        partitionColumnNames = sourceTableDesc.partitionColumnNames,
        bucketSpec = sourceTableDesc.bucketSpec)

    catalog.createTable(newTableDesc, delegate.ifNotExists)
    Seq.empty[Row]
  }
}
