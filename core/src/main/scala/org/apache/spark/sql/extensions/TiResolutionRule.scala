package org.apache.spark.sql.extensions

import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.sql.{AnalysisException, _}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  private def autoLoad = tiContext.autoLoad
  protected def meta: MetaManager = tiContext.meta
  private def tiCatalog = tiContext.tiCatalog
  private def tiSession = tiContext.tiSession
  private def sqlContext = tiContext.sqlContext

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
      new TiDBRelation(
        tiSession,
        new TiTableReference(dbName, tableName, sizeInBytes),
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
