package org.apache.spark.sql.extensions

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import com.pingcap.tispark.{MetaManager, TiDBRelation, TiTableReference}
import org.apache.spark.sql.execution.datasources.LogicalRelation

case class TiResolutionRule(getOrCreateTiContext: SparkSession => TiContext)(
  sparkSession: SparkSession
) extends Rule[LogicalPlan] {
  protected val tiContext: TiContext = getOrCreateTiContext(sparkSession)
  protected val meta: MetaManager = tiContext.meta

  protected def resolveTiDBRelation(tableIdentifier: TableIdentifier,
                                    databaseName: String): TiDBRelation =
    new TiDBRelation(
      tiContext.tiSession,
      new TiTableReference(databaseName, tableIdentifier.table),
      meta
    )(tiContext.sqlContext)

  private def getDatabaseName(tableIdentifier: TableIdentifier): String =
    tableIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case UnresolvedRelation(tableIdentifier)
        if meta.getDatabase(getDatabaseName(tableIdentifier)).isDefined =>
      LogicalRelation(resolveTiDBRelation(tableIdentifier, getDatabaseName(tableIdentifier)))
  }
}
