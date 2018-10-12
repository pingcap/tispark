package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession, TiContext}
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * TiCommand is used to inherit from [[org.apache.spark.sql.execution.command.RunnableCommand]]
 *
 * Because we are unable to extend from a case class implementing RunnableCommand, we will have
 * to extend TiCommand from its abstract class directly.
 */
abstract class TiCommand(delegate: RunnableCommand) extends RunnableCommand {
  val tiContext: TiContext
  def tiCatalog: TiSessionCatalog = tiContext.tiCatalog
  override def output: Seq[Attribute] = delegate.output
  override def children: Seq[LogicalPlan] = delegate.children
  override def run(sparkSession: SparkSession): Seq[Row] = delegate.run(sparkSession)
}