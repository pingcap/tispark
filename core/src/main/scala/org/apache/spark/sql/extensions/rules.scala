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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation, UnresolvedV2Relation}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{Command, DescribeTable, InsertIntoStatement, LogicalPlan, SetCatalogAndNamespace, ShowNamespaces}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, Identifier}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{AnalysisException, _}

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val autoLoad = tiContext.autoLoad
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext
<<<<<<< HEAD
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  protected val resolveTiDBRelation: TableIdentifier => LogicalPlan =
=======

  private def getDatabaseFromIdentifier(tableIdentifier: Seq[String]): String = {
    if (tableIdentifier.size == 1) {
      tiCatalog.getCurrentDatabase
    } else {
      tableIdentifier.head
    }
  }

  protected val resolveTiDBRelation: Seq[String] => LogicalPlan =
>>>>>>> support spark-3.0
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
      // Use SubqueryAlias so that projects and joins can correctly resolve
      // UnresolvedAttributes in JoinConditions, Projects, Filters, etc.
      newSubqueryAlias(tableName, LogicalRelation(tiDBRelation))
    }

  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp resolveTiDBRelations

  protected def resolveTiDBRelations: PartialFunction[LogicalPlan, LogicalPlan] = {
    case i @ InsertIntoStatement(UnresolvedRelation(tableIdentifier), _, _, _, _)
        if tiCatalog
          .catalogOf(if (tableIdentifier.size == 1) None else Some(tableIdentifier.head))
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      i.copy(table = EliminateSubqueryAliases(resolveTiDBRelation(tableIdentifier)))
    case UnresolvedRelation(tableIdentifier)
        if tiCatalog
          .catalogOf(if (tableIdentifier.size == 1) None else Some(tableIdentifier.head))
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      resolveTiDBRelation(tableIdentifier)
  }

  private def getDatabaseFromIdentifier(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(tiCatalog.getCurrentDatabase)
}

case class NopCommand(name: String) extends Command {}
case class TiDDLRule(getOrCreateTiContext: SparkSession => TiContext)(sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)

<<<<<<< HEAD
<<<<<<< HEAD
  override def apply(plan: LogicalPlan): LogicalPlan =
    plan transformUp {
      // TODO: support other commands that may concern TiSpark catalog.
      case sd: ShowDatabasesCommand =>
        TiShowDatabasesCommand(tiContext, sd)
      case sd: SetDatabaseCommand =>
        TiSetDatabaseCommand(tiContext, sd)
      case st: ShowTablesCommand =>
        TiShowTablesCommand(tiContext, st)
      case st: ShowColumnsCommand =>
        TiShowColumnsCommand(tiContext, st)
      case dt: DescribeTableCommand =>
        TiDescribeTablesCommand(tiContext, dt)
      case dc: DescribeColumnCommand =>
        TiDescribeColumnCommand(tiContext, dc)
      case ct: CreateTableLikeCommand =>
        TiCreateTableLikeCommand(tiContext, ct)
    }
=======
=======
  def getDBAndTableName(ident: Identifier): (String, Option[String]) = {
    ident.namespace() match {
      case Array(db) =>
        (ident.name(), Some(db))
      case _ =>
        (ident.name(), None)
    }
  }

  def createDescribeTableInfo(dt: DescribeTableCommand): DescribeTableInfo = {
    new DescribeTableInfo(
      TableIdentifier(dt.table.table, dt.table.database),
      dt.partitionSpec,
      dt.isExtended
    )
  }

  def createDescribeTableInfo(dt: DescribeTable): DescribeTableInfo = {
    dt.table match {
      case u: UnresolvedV2Relation =>
        val (name, db) = getDBAndTableName(u.tableName)
        new DescribeTableInfo(TableIdentifier(name, db), Map.empty[String, String], dt.isExtended)
      case _ =>
        new DescribeTableInfo(
          TableIdentifier(dt.table.name, None),
          Map.empty[String, String],
          dt.isExtended
        )
    }
  }

  def isSupportedCatalog(sd: SetCatalogAndNamespace): Boolean = {
    if (sd.catalogName.isEmpty)
      false
    else {
      sd.catalogName.get.equals(CatalogManager.SESSION_CATALOG_NAME)
    }
  }

>>>>>>> Enable catalog test and tpch q15 (#1234)
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
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
      TiDescribeTablesCommand(tiContext, dt, createDescribeTableInfo(dt))
    case dt: DescribeTable =>
      // use NopCommand to avoid failAnalysis
      TiDescribeTablesCommand(tiContext, NopCommand("empty"), createDescribeTableInfo(dt))
    case ct: CreateTableLikeCommand =>
      TiCreateTableLikeCommand(tiContext, ct)
  }
>>>>>>> support spark-3.0
}
