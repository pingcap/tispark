package org.apache.spark.sql.execution.command

import org.apache.spark.sql.{Row, SparkSession, TiContext}

case class TiShowDatabasesCommand(tiContext: TiContext, delegate: ShowDatabasesCommand)
    extends TiCommand(delegate) {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databases =
      // Not leveraging catalog-specific db pattern, at least Hive and Spark behave different than each other.
      delegate.databasePattern
        .map(tiCatalog.listDatabases)
        .getOrElse(tiCatalog.listDatabases())
    databases.map { d =>
      Row(d)
    }
  }
}
