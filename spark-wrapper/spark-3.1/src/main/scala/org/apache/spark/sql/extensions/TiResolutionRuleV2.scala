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

import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.TiDBTable
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{
  InsertIntoStatement,
  LogicalPlan,
  SubqueryAlias
}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

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
    case i @ InsertIntoStatement(DataSourceV2Relation(table, output, _, _, _), _, _, _, _, _)
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
