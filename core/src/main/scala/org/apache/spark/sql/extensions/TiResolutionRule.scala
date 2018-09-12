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

  // TODO: Eliminate duplicate usage of getDatabaseFromIdentifier
  private def getDatabaseFromIdentifier(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(tiCatalog.getCurrentDatabase)

  protected val resolveTiDBRelation: TableIdentifier => TiDBRelation =
    (tableIdentifier: TableIdentifier) =>
      new TiDBRelation(
        tiSession,
        new TiTableReference(getDatabaseFromIdentifier(tableIdentifier), tableIdentifier.table),
        meta
      )(sqlContext)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case UnresolvedRelation(tableIdentifier)
        if tiCatalog
          .catalogOf(Option.apply(getDatabaseFromIdentifier(tableIdentifier)))
          .exists(_.isInstanceOf[TiSessionCatalog]) =>
      LogicalRelation(resolveTiDBRelation(tableIdentifier))
  }
}
