package org.apache.spark.sql.extensions

import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._

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
  }
}
