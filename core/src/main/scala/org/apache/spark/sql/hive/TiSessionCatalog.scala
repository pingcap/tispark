/*
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import com.pingcap.tikv.meta.{TiDBInfo, TiTableInfo}
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark._
import com.pingcap.tispark.listener.CacheInvalidateListener
import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchDatabaseException, NoSuchTableException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.catalyst.{CatalystConf, TableIdentifier}

class TiSessionCatalog(externalCatalog: HiveExternalCatalog,
                       globalTempViewManager: GlobalTempViewManager,
                       sparkSession: SparkSession,
                       functionResourceLoader: FunctionResourceLoader,
                       functionRegistry: FunctionRegistry,
                       conf: CatalystConf,
                       hadoopConf: Configuration)
    extends HiveSessionCatalog(
      externalCatalog,
      globalTempViewManager,
      sparkSession,
      functionResourceLoader,
      functionRegistry,
      conf,
      hadoopConf
    ) {

  val tiConf: TiConfiguration = TiUtils.sparkConfToTiConf(sparkSession.sparkContext.getConf)
  val session: TiSession = TiSession.create(tiConf)

  val meta: MetaManager = new MetaManager(session.getCatalog)

  override def lookupRelation(tableIdent: TableIdentifier, alias: Option[String]): LogicalPlan =
    synchronized {
      val table = formatTableName(tableIdent.table)
      val db = formatDatabaseName(tableIdent.database.getOrElse(currentDb))
      if (meta.getDatabase(db).isDefined && meta.getTable(db, table).isDefined) {
        val rel: TiDBRelation =
          new TiDBRelation(session, new TiTableReference(db, table), meta)(sparkSession.sqlContext)
        val relPlan = sparkSession.sqlContext.baseRelationToDataFrame(rel).logicalPlan
        val qualifiedTable = SubqueryAlias(tableIdent.table, relPlan, None)
        alias.map(a => SubqueryAlias(a, qualifiedTable, None)).getOrElse(qualifiedTable)
      } else {
        super.lookupRelation(tableIdent, alias)
      }
    }

  override def databaseExists(db: String): Boolean = {
    val dbName = formatDatabaseName(db)
    val tiDB = meta.getDatabase(dbName)

    if (tiDB.isDefined) {
      true
    } else {
      externalCatalog.databaseExists(dbName)
    }
  }

  override def listDatabases(): Seq[String] =
    meta.getDatabases
      .map(_.getName)
      .union(super.listDatabases())

  override def listDatabases(pattern: String): Seq[String] =
    StringUtils.filterPattern(listDatabases(), pattern)

  override def tableExists(name: TableIdentifier): Boolean = synchronized {
    val db = formatDatabaseName(name.database.getOrElse(currentDb))
    val table = formatTableName(name.table)
    val tiTable = meta.getTable(db, table)

    if (tiTable.isDefined) {
      true
    } else {
      externalCatalog.tableExists(db, table)
    }
  }

  private def requireDbExists(db: String): Unit =
    if (!databaseExists(db)) {
      throw new NoSuchDatabaseException(db)
    }

  override def getDatabaseMetadata(db: String): CatalogDatabase = {
    val dbName = formatDatabaseName(db)
    requireDbExists(dbName)
    val tiDB = meta.getDatabase(dbName)
    if (tiDB.isDefined) {
      tiDBToCatalogDatabase(tiDB.get)
    } else {
      externalCatalog.getDatabase(dbName)
    }
  }

  override def getTableMetadata(name: TableIdentifier): CatalogTable = {
    val catalogTable = getTableMetadataOption(name)
    if (catalogTable.isEmpty) {
      val db = name.database.getOrElse(currentDb)
      throw new NoSuchTableException(db = db, table = name.table)
    }
    catalogTable.get
  }

  override def getTableMetadataOption(name: TableIdentifier): Option[CatalogTable] = {
    val db = formatDatabaseName(name.database.getOrElse(getCurrentDatabase))
    val table = formatTableName(name.table)
    requireDbExists(db)
    val tiTable = meta.getTable(db, table)
    if (tiTable.isDefined) {
      Option(tiTableToCatalogTable(name, tiTable.get))
    } else {
      externalCatalog.getTableOption(db, table)
    }
  }

  override def listTables(db: String, pattern: String): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val database = meta.getDatabase(dbName)
    if (database.isDefined) {
      val localTempViews = synchronized {
        StringUtils.filterPattern(tempTables.keys.toSeq, pattern).map { name =>
          TableIdentifier(name)
        }
      }
      meta.getTables(database.get).map { table =>
        TableIdentifier(table.getName, Option(dbName))
      } ++ localTempViews
    } else {
      super.listTables(db, pattern)
    }
  }

  def tiDBToCatalogDatabase(db: TiDBInfo): CatalogDatabase =
    CatalogDatabase(db.getName, "TiDB Database", null, null)

  def tiTableToCatalogTable(name: TableIdentifier, tiTable: TiTableInfo): CatalogTable =
    CatalogTable(
      name,
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty,
      TiUtils.getSchemaFromTable(tiTable),
      Option("TiDB")
    )
}
