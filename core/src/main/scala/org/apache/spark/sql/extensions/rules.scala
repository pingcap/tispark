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
package org.apache.spark.sql.extensions

import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.{AnalysisException, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val autoLoad = tiContext.autoLoad
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext

  private def getDatabaseFromIdentifier(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(tiCatalog.getCurrentDatabase)

  protected val resolveTiDBRelation: (String, String) => TiDBRelation =
    (dbName: String, tableName: String) => {
      val table = meta.getTable(dbName, tableName)
      if (table.isEmpty) {
        throw new AnalysisException(s"Table or view '$tableName' not found in database '$dbName'")
      }
      if (autoLoad) {
        StatisticsManager.loadStatisticsInfo(table.get)
      }
      val sizeInBytes = StatisticsManager.estimateTableSize(table.get)
      TiDBRelation(
        tiSession,
        TiTableReference(dbName, tableName, sizeInBytes),
        meta
      )(sqlContext)
    }

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case rel @ UnresolvedRelation(tableIdentifier) =>
      val dbName = getDatabaseFromIdentifier(tableIdentifier)
      if (!tiCatalog.isTemporaryTable(tableIdentifier) && tiCatalog
            .catalogOf(Option.apply(dbName))
            .exists(_.isInstanceOf[TiSessionCatalog])) {
        LogicalRelation(resolveTiDBRelation(dbName, tableIdentifier.table))
      } else {
        rel
      }
  }
}

case class TiDDLRule(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // TODO: support other commands that may concern TiSpark catalog.
    case sd: ShowDatabasesCommand =>
      TiShowDatabasesCommand(tiContext, sd)
    case sd: SetDatabaseCommand =>
      TiSetDatabaseCommand(tiContext, sd)
    case st: ShowTablesCommand =>
      TiShowTablesCommand(tiContext, st)
    case dt: DescribeTableCommand =>
      TiDescribeTablesCommand(tiContext, dt)
  }
}
