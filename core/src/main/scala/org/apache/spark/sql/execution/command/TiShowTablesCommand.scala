package org.apache.spark.sql.execution.command

import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.{Row, SparkSession, TiContext}

class TiShowTablesCommand(val tiContext: TiContext,
                          databaseName: Option[String],
                          tableIdentifierPattern: Option[String],
                          isExtended: Boolean,
                          partitionSpec: Option[TablePartitionSpec])
    extends ShowTablesCommand(databaseName, tableIdentifierPattern, isExtended, partitionSpec)
    with TiCommand {
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val db = databaseName.getOrElse(tiCatalog.getCurrentDatabase)
    // Show the information of tables.
    val tables =
      tableIdentifierPattern.map(tiCatalog.listTables(db, _)).getOrElse(tiCatalog.listTables(db))
    tables.map { tableIdent =>
      val database = tableIdent.database.getOrElse("")
      val tableName = tableIdent.table
      val isTemp = tiCatalog.isTemporaryTable(tableIdent)
      if (isExtended) {
        val information = tiCatalog.getTempViewOrPermanentTableMetadata(tableIdent).simpleString
        Row(database, tableName, isTemp, s"$information\n")
      } else {
        Row(database, tableName, isTemp)
      }
    }
  }
}
