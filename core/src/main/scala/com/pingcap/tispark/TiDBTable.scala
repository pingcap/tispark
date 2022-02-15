package com.pingcap.tispark

import com.pingcap.tikv.TiSession
import com.pingcap.tikv.exception.TiClientInternalException
import com.pingcap.tikv.meta.{TiTableInfo, TiTimestamp}
import com.pingcap.tispark.utils.TiUtil
import com.pingcap.tispark.write.TiDBOptions
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.sql.connector.catalog.{SupportsDelete, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

import java.util

case class TiDBTable(
    session: TiSession,
    tableRef: TiTableReference,
    meta: MetaManager,
    var ts: TiTimestamp = null,
    options: Option[TiDBOptions] = None)(@transient val tiContext: TiContext)
    extends SupportsDelete
    with SupportsWrite {

  lazy val table: TiTableInfo = getTableOrThrow(tableRef.databaseName, tableRef.tableName)

  override def deleteWhere(filters: Array[Filter]): Unit = {
    for (i <- filters.indices) {
      println(filters(i))
    }
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new TiDBWriterBuilder(info, options.get)(tiContext)

  override def name(): String = "test"

  override def schema(): StructType = {
    val test = TiUtil.getSchemaFromTable(table)
    print(test)
    test
  }

  override def capabilities(): util.Set[TableCapability] = {
    val capabilities = new util.HashSet[TableCapability]
    capabilities.add(TableCapability.BATCH_READ)
    capabilities.add(TableCapability.BATCH_WRITE)
    capabilities
  }

  private def getTableOrThrow(database: String, table: String): TiTableInfo =
    meta.getTable(database, table).getOrElse {
      val db = meta.getDatabaseFromCache(database)
      if (db.isEmpty) {
        throw new TiClientInternalException(
          "Database not exist " + database + " valid databases are: " + meta.getDatabasesFromCache
            .map(_.getName)
            .mkString("[", ",", "]"))
      } else {
        throw new TiClientInternalException(
          "Table not exist " + tableRef + " valid tables are: " + meta
            .getTablesFromCache(db.get)
            .map(_.getName)
            .mkString("[", ",", "]"))
      }
    }
}
