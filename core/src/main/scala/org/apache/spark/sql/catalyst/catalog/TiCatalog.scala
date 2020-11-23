/*
 * Copyright 2020 PingCAP, Inc.
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

import java.util

import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark.MetaManager
import com.pingcap.tispark.utils.TiUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog.{
  Identifier,
  NamespaceChange,
  SupportsNamespaces,
  Table,
  TableCapability,
  TableCatalog,
  TableChange,
  V1Table
}
import org.apache.spark.sql.connector.expressions.{FieldReference, LogicalExpressions, Transform}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * An implementation of catalog v2 `Table` to expose v1 table metadata.
 */
case class TiDBTable(v1Table: CatalogTable) extends Table {
  implicit class IdentifierHelper(identifier: TableIdentifier) {
    def quoted: String = {
      identifier.database match {
        case Some(db) =>
          Seq(db, identifier.table).map(quote).mkString(".")
        case _ =>
          quote(identifier.table)

      }
    }

    private def quote(part: String): String = {
      if (part.contains(".") || part.contains("`")) {
        s"`${part.replace("`", "``")}`"
      } else {
        part
      }
    }
  }

  def databaseName: String = v1Table.identifier.database.get
  def tableName: String = v1Table.identifier.table

  lazy val options: Map[String, String] = {
    v1Table.storage.locationUri match {
      case Some(uri) =>
        v1Table.storage.properties + ("path" -> uri.toString)
      case _ =>
        v1Table.storage.properties
    }
  }

  override lazy val capabilities: util.Set[TableCapability] = new util.HashSet[TableCapability]()
  override lazy val properties: util.Map[String, String] = v1Table.properties.asJava

  var tiTableInfo: Option[TiTableInfo] = None
  override lazy val schema: StructType = v1Table.schema

  override lazy val partitioning: Array[Transform] = {
    val partitions = new mutable.ArrayBuffer[Transform]()

    v1Table.partitionColumnNames.foreach { col =>
      partitions += LogicalExpressions.identity(FieldReference(col))
    }

    v1Table.bucketSpec.foreach { spec =>
      partitions += LogicalExpressions
        .bucket(spec.numBuckets, spec.bucketColumnNames.map(FieldReference(_)).toArray)
    }

    partitions.toArray
  }

  override def name: String = v1Table.identifier.quoted

  override def toString: String = s"UnresolvedTiDBTable($name)"
}

object TiCatalog {
  val className = {
    val fullname = getClass.getName
    fullname.substring(0, fullname.length - 1)
  }
}

class TiCatalog extends TableCatalog with SupportsNamespaces {
  var meta: Option[MetaManager] = None
  private var _name: Option[String] = None
  private var _current_namespace: Option[Array[String]] = None
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def setCurrentNamespace(namespace: Option[Array[String]]): Unit =
    synchronized {
      _current_namespace = namespace
    }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = Some(name)
    if (!options.containsKey("pd.addresses") && !options.containsKey("pd.address")) {
      throw new Exception("missing configuration spark.sql.catalog.tidb_catalog.pd.addresses")
    }
    val pdAddress = options.getOrDefault("pd.addresses", options.get("pd.address"))
    logger.info(s"Initialize TiCatalog with name: $name, pd address: $pdAddress")
    val conf = TiConfiguration.createDefault(pdAddress)
    val session = TiSession.getInstance(conf)
    meta = Some(new MetaManager(session.getCatalog))
  }

  override def name(): String = _name.get

  override def namespaceExists(namespace: Array[String]): Boolean =
    namespace match {
      case Array(db) =>
        meta.get.getDatabase(db).isDefined
      case _ =>
        false
    }

  override def listNamespaces(): Array[Array[String]] =
    meta.get.getDatabases.map(dbInfo => Array(dbInfo.getName)).toArray

  override def listNamespaces(namespace: Array[String]): Array[Array[String]] = {
    namespace match {
      case Array() =>
        listNamespaces()
      case Array(db) if meta.get.getDatabase(db).isDefined =>
        Array()
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadNamespaceMetadata(namespace: Array[String]): util.Map[String, String] = {
    namespace match {
      case Array(db) =>
        if (meta.get.getDatabase(db).isDefined) {
          new util.HashMap[String, String]()
        } else {
          throw new NoSuchNamespaceException(namespace)
        }
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  override def loadTable(ident: Identifier): Table = {
    val currentNS =
      ident.namespace() match {
        case Array(_) => ident.namespace()
        case _ =>
          _current_namespace.getOrElse(this.defaultNamespace())
      }

    val dbName =
      currentNS match {
        case Array(db) => db
        case _ => throw new NoSuchTableException(ident)
      }

    val table = meta.get
      .getTable(dbName, ident.name())
      .getOrElse(throw new NoSuchTableException(dbName, ident.name()))
    val schema = TiUtil.getSchemaFromTable(table)

    val t = CatalogTable(
      TableIdentifier(ident.name(), Some(dbName)),
      CatalogTableType.EXTERNAL,
      CatalogStorageFormat.empty,
      schema)

    val ret = TiDBTable(t)
    // add BATCH_READ to just keep compiler happy
    ret.capabilities.add(TableCapability.BATCH_READ)
    ret.tiTableInfo = Some(table)
    ret
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        meta.get
          .getTables(meta.get.getDatabase(db).getOrElse(throw new NoSuchNamespaceException(db)))
          .map(tbl => Identifier.of(Array(db), tbl.getName))
          .toArray
      case _ =>
        throw new NoSuchNamespaceException(namespace)
    }
  }

  // todo add table cache and invalidate the cached table
  override def invalidateTable(ident: Identifier): Unit = super.invalidateTable(ident)

  override def toString: String = s"TiCatalog($name)"

  // Following are unimplemented.
  override def createTable(
      ident: Identifier,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): Table = ???

  override def alterTable(ident: Identifier, changes: TableChange*): Table = ???

  override def dropTable(ident: Identifier): Boolean = ???

  override def renameTable(oldIdent: Identifier, newIdent: Identifier): Unit = ???

  override def dropNamespace(namespace: Array[String]): Boolean = ???

  override def createNamespace(
      namespace: Array[String],
      metadata: util.Map[String, String]): Unit =
    ???

  override def alterNamespace(namespace: Array[String], changes: NamespaceChange*): Unit = ???
}
