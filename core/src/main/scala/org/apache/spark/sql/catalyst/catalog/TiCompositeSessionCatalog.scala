/*
 * Copyright 2019 PingCAP, Inc.
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

package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.util.concurrent.Callable

import org.apache.spark.sql.TiContext
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.analysis.{EmptyFunctionRegistry, NoSuchDatabaseException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.types.StructType

/**
 * A composition of two catalogs that behaves as a concrete catalog.
 * @param tiContext
 */
class TiCompositeSessionCatalog(val tiContext: TiContext)
    extends SessionCatalog(
      tiContext.tiConcreteCatalog.externalCatalog,
      EmptyFunctionRegistry,
      tiContext.sqlContext.conf)
    with TiSessionCatalog {

  lazy val policy: CompositeCatalogPolicy = TiLegacyPolicy(tiContext)
  lazy val primaryCatalog: SessionCatalog = policy.primaryCatalog
  lazy val secondaryCatalog: SessionCatalog = policy.secondaryCatalog
  lazy val tiCatalog: TiSessionCatalog = policy.tiCatalog
  lazy val sessionCatalog: SessionCatalog = policy.sessionCatalog
  lazy val catalogCache = new CatalogCache

  // Following are handled by composite catalog.
  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit =
    catalogOf(Some(db))
      .getOrElse(if (!ignoreIfNotExists) throw new NoSuchDatabaseException(db) else return
      )
      .dropDatabase(db, ignoreIfNotExists, cascade)

  override def alterDatabase(dbDefinition: CatalogDatabase): Unit =
    catalogOf(Some(dbDefinition.name))
      .getOrElse(throw new NoSuchDatabaseException(dbDefinition.name))
      .alterDatabase(dbDefinition)

  override def getDatabaseMetadata(db: String): CatalogDatabase =
    catalogOf(Some(db))
      .getOrElse(throw new NoSuchDatabaseException(db))
      .getDatabaseMetadata(db)

  override def databaseExists(db: String): Boolean =
    primaryCatalog.databaseExists(db) || secondaryCatalog.databaseExists(db)

  override def listDatabases(): Seq[String] =
    (primaryCatalog.listDatabases() ++ secondaryCatalog.listDatabases()).distinct

  override def listDatabases(pattern: String): Seq[String] =
    (primaryCatalog.listDatabases(pattern) ++ secondaryCatalog.listDatabases(pattern)).distinct

  override def alterTable(tableDefinition: CatalogTable): Unit =
    catalogOf(tableDefinition.identifier.database)
      .getOrElse(throw new NoSuchDatabaseException(
        tableDefinition.identifier.database.getOrElse(getCurrentDatabase)))
      .alterTable(tableDefinition)

  override def alterTableDataSchema(
      identifier: TableIdentifier,
      newDataSchema: StructType): Unit =
    catalogOf(identifier.database)
      .getOrElse(
        throw new NoSuchDatabaseException(identifier.database.getOrElse(getCurrentDatabase)))
      .alterTableDataSchema(identifier, newDataSchema)

  override def alterTableStats(
      identifier: TableIdentifier,
      newStats: Option[CatalogStatistics]): Unit =
    catalogOf(identifier.database)
      .getOrElse(
        throw new NoSuchDatabaseException(identifier.database.getOrElse(getCurrentDatabase)))
      .alterTableStats(identifier, newStats)

  override def tableExists(name: TableIdentifier): Boolean =
    catalogOf(name.database)
      .map {
        // Need to exclude tables from Ti's default db.
        case tiCatalog: TiSessionCatalog =>
          !name.database.getOrElse(getCurrentDatabase).equals("default") && tiCatalog.tableExists(
            name)
        case catalog: SessionCatalog => catalog.tableExists(name)
      }
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))

  override def getTableMetadata(name: TableIdentifier): CatalogTable =
    catalogOf(name.database)
      .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
      .getTableMetadata(name)

  override def renameTable(oldName: TableIdentifier, newName: TableIdentifier): Unit =
    if (isTemporaryTable(oldName)) {
      sessionCatalog.renameTable(oldName, newName)
    } else {
      catalogOf(oldName.database)
        .getOrElse(
          throw new NoSuchDatabaseException(oldName.database.getOrElse(getCurrentDatabase)))
        .renameTable(oldName, newName)
    }

  override def dropTable(
      name: TableIdentifier,
      ignoreIfNotExists: Boolean,
      purge: Boolean): Unit =
    if (isTemporaryTable(name)) {
      sessionCatalog.dropTable(name, ignoreIfNotExists, purge)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .dropTable(name, ignoreIfNotExists, purge)
    }

  override def lookupRelation(name: TableIdentifier): LogicalPlan =
    if (isTemporaryTable(name)) {
      sessionCatalog.lookupRelation(name)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .lookupRelation(name)
    }

  override def listTables(
      db: String,
      pattern: String,
      includeLocalTempViews: Boolean): Seq[TableIdentifier] = {
    val dbName = formatDatabaseName(db)
    val dbTables = catalogOf(Some(dbName))
      .getOrElse(throw new NoSuchDatabaseException(dbName)) match {
      case _: TiSessionCatalog =>
        val tempViews = if (dbName == globalTempDB) {
          sessionCatalog
            .listTables(dbName, pattern)
        } else {
          sessionCatalog
            .listTables(sessionCatalog.getCurrentDatabase, pattern)
            .filter(sessionCatalog.isTemporaryTable)
        }
        val tables = tiCatalog.listTables(db, pattern)
        tables ++ tempViews
      case catalog =>
        catalog.listTables(db, pattern)
    }

    if (includeLocalTempViews) {
      dbTables ++ listLocalTempViews(pattern)
    } else {
      dbTables
    }
  }

  override def getTempViewOrPermanentTableMetadata(name: TableIdentifier): CatalogTable =
    if (isTemporaryTable(name)) {
      sessionCatalog.getTempViewOrPermanentTableMetadata(name)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .getTempViewOrPermanentTableMetadata(name)
    }

  // Following are routed to Ti catalog.
  override def catalogOf(database: Option[String]): Option[SessionCatalog] =
    synchronized {
      database
        .map(
          db =>
            // Global temp db is special, route to session catalog.
            if (db == globalTempDB) {
              Some(sessionCatalog)
            } else {
              Seq(primaryCatalog, secondaryCatalog).find(_.databaseExists(db))
            })
        .getOrElse(Some(currentCatalog))
    }

  override def getCurrentDatabase: String = currentCatalog.getCurrentDatabase

  // Used to manage catalog change by setting current database.
  private def currentCatalog: SessionCatalog = catalogCache.getCatalog

  override def setCurrentDatabase(db: String): Unit =
    synchronized {
      catalogCache.setCatalog(
        catalogOf(Some(db)).getOrElse(throw new NoSuchDatabaseException(db)))
      currentCatalog.setCurrentDatabase(db)
    }

  override def isTemporaryTable(name: TableIdentifier): Boolean =
    sessionCatalog.isTemporaryTable(name)

  // Following are all routed to primary catalog.
  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit =
    sessionCatalog.createDatabase(dbDefinition, ignoreIfExists)

  override def getCachedPlan(t: QualifiedTableName, c: Callable[LogicalPlan]): LogicalPlan =
    sessionCatalog.getCachedPlan(t, c)

  override def getCachedTable(key: QualifiedTableName): LogicalPlan =
    sessionCatalog.getCachedTable(key)

  override def cacheTable(t: QualifiedTableName, l: LogicalPlan): Unit =
    sessionCatalog.cacheTable(t, l)

  override def invalidateCachedTable(key: QualifiedTableName): Unit =
    sessionCatalog.invalidateCachedTable(key)

  override def invalidateAllCachedTables(): Unit = sessionCatalog.invalidateAllCachedTables()

  override def getDefaultDBPath(db: String): URI = sessionCatalog.getDefaultDBPath(db)

  override def createTable(
      tableDefinition: CatalogTable,
      ignoreIfExists: Boolean,
      validateLocation: Boolean = true): Unit =
    sessionCatalog.createTable(tableDefinition, ignoreIfExists, validateLocation)

  override def loadTable(
      name: TableIdentifier,
      loadPath: String,
      isOverwrite: Boolean,
      isSrcLocal: Boolean): Unit =
    sessionCatalog.loadTable(name, loadPath, isOverwrite, isSrcLocal)

  override def loadPartition(
      name: TableIdentifier,
      loadPath: String,
      spec: TablePartitionSpec,
      isOverwrite: Boolean,
      inheritTableSpecs: Boolean,
      isSrcLocal: Boolean): Unit =
    sessionCatalog.loadPartition(name, loadPath, spec, isOverwrite, inheritTableSpecs, isSrcLocal)

  override def defaultTablePath(tableIdent: TableIdentifier): URI =
    sessionCatalog.defaultTablePath(tableIdent)

  override def createTempView(
      name: String,
      tableDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit =
    sessionCatalog.createTempView(name, tableDefinition, overrideIfExists)

  override def createGlobalTempView(
      name: String,
      viewDefinition: LogicalPlan,
      overrideIfExists: Boolean): Unit =
    sessionCatalog.createGlobalTempView(name, viewDefinition, overrideIfExists)

  override def alterTempViewDefinition(
      name: TableIdentifier,
      viewDefinition: LogicalPlan): Boolean =
    sessionCatalog.alterTempViewDefinition(name, viewDefinition)

  override def getTempView(name: String): Option[LogicalPlan] = sessionCatalog.getTempView(name)

  override def getGlobalTempView(name: String): Option[LogicalPlan] =
    sessionCatalog.getGlobalTempView(name)

  override def dropTempView(name: String): Boolean = sessionCatalog.dropTempView(name)

  override def dropGlobalTempView(name: String): Boolean = sessionCatalog.dropGlobalTempView(name)

  override def refreshTable(name: TableIdentifier): Unit =
    if (isTemporaryTable(name)) {
      sessionCatalog.refreshTable(name)
    } else {
      catalogOf(name.database)
        .getOrElse(throw new NoSuchDatabaseException(name.database.getOrElse(getCurrentDatabase)))
        .refreshTable(name)
    }

  override def clearTempTables(): Unit = sessionCatalog.clearTempTables()

  override def createPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition],
      ignoreIfExists: Boolean): Unit =
    sessionCatalog.createPartitions(tableName, parts, ignoreIfExists)

  override def dropPartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      ignoreIfNotExists: Boolean,
      purge: Boolean,
      retainData: Boolean): Unit =
    sessionCatalog.dropPartitions(tableName, specs, ignoreIfNotExists, purge, retainData)

  override def renamePartitions(
      tableName: TableIdentifier,
      specs: Seq[TablePartitionSpec],
      newSpecs: Seq[TablePartitionSpec]): Unit =
    sessionCatalog.renamePartitions(tableName, specs, newSpecs)

  override def alterPartitions(
      tableName: TableIdentifier,
      parts: Seq[CatalogTablePartition]): Unit =
    sessionCatalog.alterPartitions(tableName, parts)

  override def getPartition(
      tableName: TableIdentifier,
      spec: TablePartitionSpec): CatalogTablePartition =
    sessionCatalog.getPartition(tableName, spec)

  override def listPartitionNames(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec]): Seq[String] =
    sessionCatalog.listPartitionNames(tableName, partialSpec)

  override def listPartitions(
      tableName: TableIdentifier,
      partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] =
    sessionCatalog.listPartitions(tableName, partialSpec)

  override def listPartitionsByFilter(
      tableName: TableIdentifier,
      predicates: Seq[Expression]): Seq[CatalogTablePartition] =
    sessionCatalog.listPartitionsByFilter(tableName, predicates)

  override def createFunction(funcDefinition: CatalogFunction, ignoreIfExists: Boolean): Unit =
    sessionCatalog.createFunction(funcDefinition, ignoreIfExists)

  override def dropFunction(name: FunctionIdentifier, ignoreIfNotExists: Boolean): Unit =
    sessionCatalog.dropFunction(name, ignoreIfNotExists)

  override def alterFunction(funcDefinition: CatalogFunction): Unit =
    sessionCatalog.alterFunction(funcDefinition)

  override def getFunctionMetadata(name: FunctionIdentifier): CatalogFunction =
    sessionCatalog.getFunctionMetadata(name)

  override def functionExists(name: FunctionIdentifier): Boolean =
    sessionCatalog.functionExists(name)

  override def loadFunctionResources(resources: Seq[FunctionResource]): Unit =
    sessionCatalog.loadFunctionResources(resources)

  override def registerFunction(
      funcDefinition: CatalogFunction,
      overrideIfExists: Boolean,
      functionBuilder: Option[FunctionBuilder]): Unit =
    sessionCatalog.registerFunction(funcDefinition, overrideIfExists, functionBuilder)

  override def dropTempFunction(name: String, ignoreIfNotExists: Boolean): Unit =
    sessionCatalog.dropTempFunction(name, ignoreIfNotExists)

  override def isTemporaryFunction(name: FunctionIdentifier): Boolean =
    sessionCatalog.isTemporaryFunction(name)

  override def lookupFunctionInfo(name: FunctionIdentifier): ExpressionInfo =
    sessionCatalog.lookupFunctionInfo(name)

  override def lookupFunction(name: FunctionIdentifier, children: Seq[Expression]): Expression =
    sessionCatalog.lookupFunction(name, children)

  override def listFunctions(db: String): Seq[(FunctionIdentifier, String)] =
    sessionCatalog.listFunctions(db)

  override def listFunctions(db: String, pattern: String): Seq[(FunctionIdentifier, String)] =
    sessionCatalog.listFunctions(db, pattern)

  override def reset(): Unit = sessionCatalog.reset()

  override private[sql] def copyStateTo(target: SessionCatalog): Unit =
    sessionCatalog.copyStateTo(target)

  /**
   * Policy of operating composite catalog, with one TiCatalog being either primary or secondary catalog.
   */
  trait CompositeCatalogPolicy {
    val primaryCatalog: SessionCatalog
    val secondaryCatalog: SessionCatalog
    val tiCatalog: TiSessionCatalog
    val sessionCatalog: SessionCatalog
  }

  /**
   * Legacy catalog first policy.
   * @param tiContext
   */
  case class TiLegacyPolicy(tiContext: TiContext) extends CompositeCatalogPolicy {
    override val primaryCatalog: SessionCatalog = tiContext.sessionCatalog
    override val secondaryCatalog: SessionCatalog = tiContext.tiConcreteCatalog
    override val tiCatalog: TiSessionCatalog = tiContext.tiConcreteCatalog
    override val sessionCatalog: SessionCatalog = tiContext.sessionCatalog
  }

  class CatalogCache {
    private var _catalog: SessionCatalog = _
    private[catalog] def getCatalog: SessionCatalog = {
      if (_catalog == null) {
        _catalog = primaryCatalog
      }
      _catalog
    }
    private[catalog] def setCatalog(catalog: SessionCatalog): Unit =
      _catalog = catalog
  }
}
