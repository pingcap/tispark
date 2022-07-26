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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.catalog

import com.pingcap.tikv.{ClientSession, TiConfiguration}

import com.pingcap.tispark.auth.TiAuthorization
import com.pingcap.tispark.v2.TiDBTable
import com.pingcap.tispark.{MetaManager, TiTableReference}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.{NoSuchNamespaceException, NoSuchTableException}
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.slf4j.LoggerFactory

import java.util

object TiCatalog {
  val className = {
    val fullname = getClass.getName
    fullname.substring(0, fullname.length - 1)
  }
}

class TiCatalog extends TableCatalog with SupportsNamespaces {
  private var clientSession: Option[ClientSession] = None
  var meta: Option[MetaManager] = None
  private var _name: Option[String] = None
  private var _current_namespace: Option[Array[String]] = None
  private val logger = LoggerFactory.getLogger(getClass.getName)
  private lazy final val tiAuthorization: Option[TiAuthorization] =
    TiAuthorization.tiAuthorization
  def setCurrentNamespace(namespace: Option[Array[String]]): Unit =
    synchronized {
      _current_namespace = namespace
    }

  override def initialize(name: String, options: CaseInsensitiveStringMap): Unit = {
    _name = Some(name)

    val pdAddress: String =
      if (TiAuthorization.enableAuth) {
        tiAuthorization.get.getPDAddress()
      } else {
        if (!options.containsKey("pd.addresses") && !options.containsKey("pd.address")) {
          throw new Exception("missing configuration spark.sql.catalog.tidb_catalog.pd.addresses")
        }
        options.getOrDefault("pd.addresses", options.get("pd.address"))
      }

    logger.info(s"Initialize TiCatalog with name: $name, pd address: $pdAddress")
    val conf = TiConfiguration.createDefault(pdAddress)
    val clientSession = ClientSession.getInstance(conf)
    meta = Some(new MetaManager(clientSession.getCatalog))
    this.clientSession = Some(clientSession)
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
    meta.get.getDatabases
      .filter(db => TiAuthorization.checkVisible(db.getName, "", tiAuthorization))
      .map(dbInfo => Array(dbInfo.getName))
      .toArray

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

    TiAuthorization.authorizeForDescribeTable(ident.name, dbName, tiAuthorization)

    val table = meta.get
      .getTable(dbName, ident.name)
      .getOrElse(throw new NoSuchTableException(dbName, ident.name))

    TiDBTable(clientSession.get, TiTableReference(dbName, ident.name), table)(
      SparkSession.active.sqlContext)
  }

  override def listTables(namespace: Array[String]): Array[Identifier] = {
    namespace match {
      case Array(db) =>
        meta.get
          .getTables(meta.get.getDatabase(db).getOrElse(throw new NoSuchNamespaceException(db)))
          .filter(tbl => TiAuthorization.checkVisible(db, tbl.getName, tiAuthorization))
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
