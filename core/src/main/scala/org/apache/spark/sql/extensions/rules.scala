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
import com.pingcap.tispark.utils.ReflectionUtil._
import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{TiDBTable, TiSessionCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoStatement, LogicalPlan, SetCatalogAndNamespace}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, SetCatalogAndNamespaceExec}
import org.apache.spark.sql.{AnalysisException, _}

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private lazy val autoLoad = tiContext.autoLoad
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext

  private def getDatabaseFromIdentifier(tableIdentifier: Seq[String]): String = {
    if (tableIdentifier.size == 1) {
      tiCatalog.getCurrentDatabase
    } else {
      tableIdentifier.head
    }
  }

  protected val resolveTiDBRelation: (TiDBTable, Seq[AttributeReference]) => LogicalPlan =
    (tiTable, output) => {
      if (autoLoad) {
        StatisticsManager.loadStatisticsInfo(tiTable.tiTableInfo.get)
      }
      val sizeInBytes = StatisticsManager.estimateTableSize(tiTable.tiTableInfo.get)
      val tiDBRelation = TiDBRelation(
        tiSession,
        TiTableReference(tiTable.dbName.get, tiTable.tiTableInfo.get.getName, sizeInBytes),
        meta
      )(sqlContext)
      // Use SubqueryAlias so that projects and joins can correctly resolve
      // UnresolvedAttributes in JoinConditions, Projects, Filters, etc.
      // todo since there is no UnresolvedAttributes, do we still need the subqueryAlias relation???
      newSubqueryAlias(
        tiTable.tiTableInfo.get.getName,
        LogicalRelation(tiDBRelation, output, None, false)
      )
    }

  protected def resolveTiDBRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
    case i @ InsertIntoStatement(DataSourceV2Relation(table, output, _), _, _, _, _)
        if table.isInstanceOf[TiDBTable] =>
      val tiTable = table.asInstanceOf[TiDBTable]
      i.copy(table = EliminateSubqueryAliases(resolveTiDBRelation(tiTable, output)))
    case DataSourceV2Relation(table, output, _) if table.isInstanceOf[TiDBTable] =>
      val tiTable = table.asInstanceOf[TiDBTable]
      resolveTiDBRelation(tiTable, output)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp resolveTiDBRelations
}

case class TiDDLRule(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // TODO: support other commands that may concern TiSpark catalog.
    //case sd: ShowDatabasesCommand =>
    //  TiShowDatabasesCommand(tiContext, sd)
    case sd: SetCatalogAndNamespace =>
      TiSetDatabaseCommand(tiContext, sd)
    //case st: ShowTablesCommand =>
    //  TiShowTablesCommand(tiContext, st)
    //case st: ShowColumnsCommand =>
    //  TiShowColumnsCommand(tiContext, st)
    //case dt: DescribeTableCommand =>
    //  TiDescribeTablesCommand(tiContext, dt)
    //case ct: CreateTableLikeCommand =>
    //  TiCreateTableLikeCommand(tiContext, ct)
  }
}
