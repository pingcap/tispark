package org.apache.spark.sql.extensions

import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import com.pingcap.tispark.statistics.StatisticsManager
import com.pingcap.tispark.utils.ReflectionUtil.newSubqueryAlias
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{EliminateSubqueryAliases, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.{InsertIntoTable, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.{AnalysisException, SparkSession, TiContext}

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
    sparkSession: SparkSession)
    extends Rule[LogicalPlan] {
  protected lazy val meta: MetaManager = tiContext.meta
  private lazy val autoLoad = tiContext.autoLoad
  private lazy val tiCatalog = tiContext.tiCatalog
  private lazy val tiSession = tiContext.tiSession
  private lazy val sqlContext = tiContext.sqlContext
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  protected val resolveTiDBRelation: TableIdentifier => LogicalPlan =
    tableIdentifier => {
      val dbName = getDatabaseFromIdentifier(tableIdentifier)
      val tableName = tableIdentifier.table
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
    case i @ InsertIntoTable(UnresolvedRelation(tableIdentifier), _, _, _, _)
        if tiCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      i.copy(table = EliminateSubqueryAliases(resolveTiDBRelation(tableIdentifier)))
    case UnresolvedRelation(tableIdentifier)
        if tiCatalog
          .catalogOf(tableIdentifier.database)
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      resolveTiDBRelation(tableIdentifier)
  }

  private def getDatabaseFromIdentifier(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(tiCatalog.getCurrentDatabase)
}