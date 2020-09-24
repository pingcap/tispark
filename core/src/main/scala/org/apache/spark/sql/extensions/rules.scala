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

import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, _}
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.catalyst.analysis.{
  EliminateSubqueryAliases,
  UnresolvedRelation,
  UnresolvedTableOrView
}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes._
import org.apache.spark.sql.catalyst.catalog.{TiDBTable, TiSessionCatalog}
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.{AnalysisException, _}
import org.slf4j.LoggerFactory

class TiResolutionRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Rule[LogicalPlan]) {
  private val logger = LoggerFactory.getLogger(getClass.getName)
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    if (TiExtensions.catalogPluginMode(sparkSession)) {
      // set the class loader to Reflection class loader to avoid class not found exception while loading TiCatalog
      logger.info("TiSpark running in catalog plugin mode")
      TiResolutionRuleV2(getOrCreateTiContext)(sparkSession)
    } else {
      TiResolutionRule(getOrCreateTiContext)(sparkSession)
    }
  }
}

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
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
        SubqueryAlias(tableName, LogicalRelation(tiDBRelation))
      } else {
        LogicalRelation(tiDBRelation)
      }
    }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp resolveTiDBRelations

  protected def resolveTiDBRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
    case i @ InsertIntoStatement(UnresolvedRelation(tableIdentifier), _, _, _, _)
        if tiCatalog
          .catalogOf(tableIdentifier)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      i.copy(table = EliminateSubqueryAliases(resolveTiDBRelation()(tableIdentifier)))
    case UnresolvedRelation(tableIdentifier)
        if tiCatalog
          .catalogOf(tableIdentifier)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      resolveTiDBRelation()(tableIdentifier)
    case UnresolvedTableOrView(tableIdentifier)
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

class TiDDLRuleFactory(getOrCreateTiContext: SparkSession => TiContext)
    extends (SparkSession => Rule[LogicalPlan]) {
  override def apply(sparkSession: SparkSession): Rule[LogicalPlan] = {
    if (TiExtensions.catalogPluginMode(sparkSession)) {
      TiDDLRuleV2(getOrCreateTiContext)(sparkSession)
    } else {
      TiDDLRule(getOrCreateTiContext)(sparkSession)
    }
  }
}

case class NopCommand(name: String) extends Command {}
case class TiDDLRule(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession)
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
      case sd: SetCatalogAndNamespace if isSupportedCatalog(sd) =>
        TiSetDatabaseCommand(tiContext, sd)
      case st: ShowTablesCommand =>
        TiShowTablesCommand(tiContext, st)
      case st: ShowColumnsCommand =>
        TiShowColumnsCommand(tiContext, st)
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
      case ct: CreateTableLikeCommand =>
        TiCreateTableLikeCommand(tiContext, ct)
    }
}

case class TiResolutionRuleV2(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val autoLoad = tiContext.autoLoad
  //private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)

  protected val resolveTiDBRelation: (TiDBTable, Seq[AttributeReference]) => LogicalPlan =
    (tiTable, output) => {
      if (autoLoad) {
        StatisticsManager.loadStatisticsInfo(tiTable.tiTableInfo.get)
      }
      val sizeInBytes = StatisticsManager.estimateTableSize(tiTable.tiTableInfo.get)
      val tiDBRelation = TiDBRelation(
        tiSession,
        TiTableReference(tiTable.databaseName, tiTable.tableName, sizeInBytes),
        meta)(sqlContext)
      // Use SubqueryAlias so that projects and joins can correctly resolve
      // UnresolvedAttributes in JoinConditions, Projects, Filters, etc.
      // todo since there is no UnresolvedAttributes, do we still need the subqueryAlias relation???
      SubqueryAlias(tiTable.tableName, LogicalRelation(tiDBRelation, output, None, false))
    }

  protected def resolveTiDBRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
    // todo can remove this branch since the target table of insert into statement should never be a tidb table
    case i @ InsertIntoStatement(DataSourceV2Relation(table, output, _, _, _), _, _, _, _)
        if table.isInstanceOf[TiDBTable] =>
      val tiTable = table.asInstanceOf[TiDBTable]
      i.copy(table = EliminateSubqueryAliases(resolveTiDBRelation(tiTable, output)))
    case DataSourceV2Relation(table, output, _, _, _) if table.isInstanceOf[TiDBTable] =>
      val tiTable = table.asInstanceOf[TiDBTable]
      resolveTiDBRelation(tiTable, output)
  }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan match {
      case _ =>
        plan transformUp resolveTiDBRelations
    }
}

case class TiDDLRuleV2(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan
}
