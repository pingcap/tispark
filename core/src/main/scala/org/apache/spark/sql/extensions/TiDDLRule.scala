package org.apache.spark.sql.extensions

import org.apache.spark.sql.{SparkSession, TiContext, TiExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command._

case class TiDDLRule(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  protected lazy val tiContext: TiContext = TiExtensions.getOrCreateTiContext(sparkSession)

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    // TODO: support other commands that may concern TiSpark catalog.
    case ShowDatabasesCommand(databasePattern) =>
      new TiShowDatabasesCommand(tiContext, databasePattern)
    case SetDatabaseCommand(databaseName) =>
      TiSetDatabaseCommand(tiContext, databaseName)
  }
}
