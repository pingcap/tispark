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

import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.{MetaManager, TiDBRelation}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{SparkSession, TiContext}

/**
 * Only work for table v2(catalog plugin)
 */
case class TiAuthorizationRule(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {

  protected lazy val meta: MetaManager = tiContext.meta
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val tiAuthorization: Option[TiAuthorization] = tiContext.tiAuthorization

  protected def checkForAuth: PartialFunction[LogicalPlan, LogicalPlan] = {
    case sa @ SubqueryAlias(identifier, child) =>
      if (identifier.qualifier.nonEmpty && identifier.qualifier.head.equals("tidb_catalog")) {
        TiAuthorization.authorizeForSelect(
          identifier.name,
          identifier.qualifier.last,
          tiAuthorization)
      }
      sa
    case sd: ShowNamespaces =>
      sd
    case sd @ SetCatalogAndNamespace(catalogManager, catalogName, namespace)
        if (catalogName.nonEmpty && catalogName.get.equals(
          "tidb_catalog") && namespace.isDefined) =>
      namespace.get
        .foreach(TiAuthorization.authorizeForSetDatabase(_, tiAuthorization))
      sd
    case st: ShowTablesCommand =>
      st
    case st @ ShowColumnsCommand(databaseName, tableName) =>
      TiAuthorization.authorizeForDescribeTable(
        tableName.table,
        tiContext.getDatabaseFromOption(databaseName),
        tiAuthorization)
      st
    case dt @ DescribeTableCommand(table, _, _) =>
      TiAuthorization.authorizeForDescribeTable(
        table.table,
        tiContext.getDatabaseFromOption(table.database),
        tiAuthorization)
      dt
    case dt @ DescribeRelation(
          LogicalRelation(TiDBRelation(_, tableRef, _, _, _), _, _, _),
          _,
          _) =>
      TiAuthorization.authorizeForDescribeTable(
        tableRef.tableName,
        tableRef.databaseName,
        tiAuthorization)
      dt
    case dc @ DescribeColumnCommand(table, _, _) =>
      TiAuthorization.authorizeForDescribeTable(
        table.table,
        tiContext.getDatabaseFromOption(table.database),
        tiAuthorization)
      dc
    case ct @ CreateTableLikeCommand(target, source, _, _, _, _) =>
      TiAuthorization.authorizeForCreateTableLike(
        tiContext.getDatabaseFromOption(target.database),
        target.table,
        tiContext.getDatabaseFromOption(source.database),
        source.table,
        tiContext.tiAuthorization)
      TiCreateTableLikeCommand(tiContext, ct)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp checkForAuth
}
