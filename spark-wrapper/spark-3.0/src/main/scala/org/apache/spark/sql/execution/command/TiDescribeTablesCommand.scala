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
package org.apache.spark.sql.execution.command

import com.pingcap.tispark.utils.ReflectionUtil.newAttributeReference
import org.apache.spark.sql.{AnalysisException, Row, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, SessionCatalog, TiSessionCatalog}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.Command
import org.apache.spark.sql.types.{MetadataBuilder, StringType, StructType}

import scala.collection.mutable.ArrayBuffer

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
    newAttributeReference(
      "col_name",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build()),
    newAttributeReference(
      "data_type",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "data type of the column").build()),
    newAttributeReference(
      "nullable",
      StringType,
      nullable = false,
      new MetadataBuilder().putString("comment", "whether the column is nullable").build()),
    newAttributeReference(
      "comment",
      StringType,
      nullable = true,
      new MetadataBuilder().putString("comment", "comment of the column").build()))

  override def run(sparkSession: SparkSession): Seq[Row] =
    tiCatalog
      .catalogOf(tableInfo.tableName.database)
      .getOrElse(throw new NoSuchDatabaseException(
        tableInfo.tableName.database.getOrElse(tiCatalog.getCurrentDatabase))) match {
      case _: TiSessionCatalog =>
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
