package org.apache.spark.sql.extensions

import org.apache.spark.sql._
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
  protected def meta: MetaManager = tiContext.meta
  private def tiCatalog = tiContext.tiCatalog
  private def tiSession = tiContext.tiSession
  private def sqlContext = tiContext.sqlContext

  private def getDatabaseFromIdentifier(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(tiCatalog.getCurrentDatabase)

  protected val resolveTiDBRelation: (TableIdentifier, String) => TiDBRelation =
    (tableIdentifier: TableIdentifier, dbName: String) =>
      new TiDBRelation(
        tiSession,
        new TiTableReference(dbName, tableIdentifier.table),
        meta
      )(sqlContext)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case rel @ UnresolvedRelation(tableIdentifier) =>
      val dbName = getDatabaseFromIdentifier(tableIdentifier)
      if (!tiCatalog.isTemporaryTable(tableIdentifier) && tiCatalog
            .catalogOf(Option.apply(dbName))
            .exists(_.isInstanceOf[TiSessionCatalog])) {
        LogicalRelation(resolveTiDBRelation(tableIdentifier, dbName))
      } else {
        rel
      }
  }
}
