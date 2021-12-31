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
import com.pingcap.tispark.statistics.StatisticsManager
import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import org.apache.spark.sql.catalyst.analysis.{
  EliminateSubqueryAliases,
  UnresolvedRelation,
  UnresolvedTableOrView
}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{
  InsertIntoStatement,
  LogicalPlan,
  SubqueryAlias
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{AnalysisException, SparkSession, TiContext}

case class TiResolutionRule(
    getOrCreateTiContext: SparkSession => TiContext,
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val autoLoad = tiContext.autoLoad
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  protected def resolveTiDBRelation(
      withSubQueryAlias: Boolean = true): Seq[String] => LogicalPlan =
    tableIdentifier => {
      val dbName = getDatabaseFromIdentifier(tableIdentifier)
      val tableName =
        if (tableIdentifier.size == 1) tableIdentifier.head else tableIdentifier.tail.head
      val table = meta.getTable(dbName, tableName)
      if (table.isEmpty) {
        throw new AnalysisException(s"Table or view '$tableName' not found in database '$dbName'")
      }
      if (autoLoad) {
        StatisticsManager.loadStatisticsInfo(table.get)
      }
      val sizeInBytes = StatisticsManager.estimateTableSize(table.get)
      val tiDBRelation =
        TiDBRelation(tiSession, TiTableReference(dbName, tableName, sizeInBytes), meta)(
          sqlContext)
      if (withSubQueryAlias) {
        // Use SubqueryAlias so that projects and joins can correctly resolve
        // UnresolvedAttributes in JoinConditions, Projects, Filters, etc.

        // Authorize for Select statement
        TiAuthorization.authorizeForSelect(tableName, dbName, tiContext.tiAuthorization)

        SubqueryAlias(tableName, LogicalRelation(tiDBRelation))
      } else {
        LogicalRelation(tiDBRelation)
      }
    }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp resolveTiDBRelations

  protected def resolveTiDBRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
    case i @ InsertIntoStatement(UnresolvedRelation(tableIdentifier, _, _), _, _, _, _, _)
        if tiCatalog
          .catalogOf(tableIdentifier)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      i.copy(table = EliminateSubqueryAliases(resolveTiDBRelation()(tableIdentifier)))
    case UnresolvedRelation(tableIdentifier, _, _)
        if tiCatalog
          .catalogOf(tableIdentifier)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      resolveTiDBRelation()(tableIdentifier)
    case UnresolvedTableOrView(tableIdentifier, _, _)
        if tiCatalog
          .catalogOf(tableIdentifier)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      resolveTiDBRelation(false)(tableIdentifier)
  }

  private def getDatabaseFromIdentifier(tableIdentifier: Seq[String]): String = {
    if (tableIdentifier.size == 1) {
      tiCatalog.getCurrentDatabase
    } else {
      tableIdentifier.head
    }
  }
}
