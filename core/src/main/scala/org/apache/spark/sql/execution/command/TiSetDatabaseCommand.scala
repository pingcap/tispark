package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession, TiContext}

case class TiSetDatabaseCommand(tiContext: TiContext, databaseName: String)
    extends RunnableCommand
    with TiCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    tiCatalog.setCurrentDatabase(databaseName)
    Seq.empty[Row]
  }
}
