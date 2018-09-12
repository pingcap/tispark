package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.{Row, SparkSession, TiContext}

class TiShowDatabasesCommand(val tiContext: TiContext, databasePattern: Option[String])
    extends ShowDatabasesCommand(databasePattern)
    with TiCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databases =
      // Not leveraging catalog-specific db pattern, at least Hive and Spark behave different than each other.
      databasePattern
        .map(StringUtils.filterPattern(tiCatalog.listDatabases(), _))
        .getOrElse(tiCatalog.listDatabases())
    databases.map { d =>
      Row(d)
    }
  }
}
