package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession, TiContext}

case class TiSetDatabaseCommand(tiContext: TiContext, delegate: SetDatabaseCommand)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    tiCatalog.setCurrentDatabase(delegate.databaseName)
    Seq.empty[Row]
  }
}
