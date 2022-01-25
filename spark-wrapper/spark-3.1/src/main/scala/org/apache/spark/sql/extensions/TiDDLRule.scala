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

package org.apache.spark.sql.extensions

import com.pingcap.tispark.TiDBRelation
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{
  DescribeColumn,
  DescribeRelation,
  LogicalPlan,
  SetCatalogAndNamespace,
  ShowColumns,
  ShowNamespaces
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier}
import org.apache.spark.sql.execution.command.{
  CreateTableLikeCommand,
  DescribeColumnCommand,
  DescribeTableCommand,
  DescribeTableInfo,
  ShowColumnsCommand,
  ShowTablesCommand,
  TiCreateTableLikeCommand,
  TiDescribeColumnCommand,
  TiDescribeTablesCommand,
  TiSetDatabaseCommand,
  TiShowColumnsCommand,
  TiShowDatabasesCommand,
  TiShowTablesCommand
}
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class TiDDLRule(getOrCreateTiContext: SparkSession => TiContext, sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  def getDBAndTableName(ident: Identifier): (String, Option[String]) = {
    ident.namespace() match {
      case Array(db) =>
        (ident.name(), Some(db))
      case _ =>
        (ident.name(), None)
    }
  }

  def isSupportedCatalog(sd: SetCatalogAndNamespace): Boolean = {
    if (sd.catalogName.isEmpty)
      false
    else {
      sd.catalogName.get.equals(CatalogManager.SESSION_CATALOG_NAME)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      // TODO: support other commands that may concern TiSpark catalog.
      case sd: ShowNamespaces =>
        TiShowDatabasesCommand(tiContext, sd)
      case sd @ SetCatalogAndNamespace(catalogManager, catalogName, namespace) if isSupportedCatalog(sd) && namespace.isDefined =>
        TiSetDatabaseCommand(tiContext, sd)
      case st: ShowTablesCommand =>
        TiShowTablesCommand(tiContext, st)
      case st: ShowColumnsCommand =>
        TiShowColumnsCommand(tiContext, st)
      case ShowColumns(LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _), _) =>
        TiShowColumnsCommand(
          tiContext,
          ShowColumnsCommand(
            None,
            new TableIdentifier(tableRef.tableName, Some(tableRef.databaseName))))
      case dt: DescribeTableCommand =>
        TiDescribeTablesCommand(
          tiContext,
          dt,
          DescribeTableInfo(
            TableIdentifier(dt.table.table, dt.table.database),
            dt.partitionSpec,
            dt.isExtended))
      case dt @ DescribeRelation(
            LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
            _,
            _) =>
        TiDescribeTablesCommand(
          tiContext,
          dt,
          DescribeTableInfo(
            TableIdentifier(tableRef.tableName, Some(tableRef.databaseName)),
            dt.partitionSpec,
            dt.isExtended))
      case dc: DescribeColumnCommand =>
        TiDescribeColumnCommand(tiContext, dc)
      case DescribeColumn(
            LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
            colNameParts,
            isExtended) =>
        TiDescribeColumnCommand(
          tiContext,
          DescribeColumnCommand(
            TableIdentifier(tableRef.tableName, Some(tableRef.databaseName)),
            colNameParts,
            isExtended))
      case ct: CreateTableLikeCommand =>
        TiCreateTableLikeCommand(tiContext, ct)
    }
}
