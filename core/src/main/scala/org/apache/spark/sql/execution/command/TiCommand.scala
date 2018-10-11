package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

abstract class TiCommand extends RunnableCommand {
  val tiContext: TiContext
  def tiCatalog: TiSessionCatalog = tiContext.tiCatalog
}

abstract class TiDelegateCommand(delegate: RunnableCommand) extends TiCommand {
  override def output: Seq[Attribute] = delegate.output
  override def children: Seq[LogicalPlan] = delegate.children
  override def run(sparkSession: SparkSession): Seq[Row] = delegate.run(sparkSession)
}