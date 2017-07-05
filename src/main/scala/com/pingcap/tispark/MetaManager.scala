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

package com.pingcap.tispark

import com.pingcap.tikv.catalog.Catalog
import com.pingcap.tikv.meta.{TiDBInfo, TiTableInfo}
import com.pingcap.tikv.{TiCluster, TiConfiguration}

import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.collection.breakOut

// Likely this needs to be merge to client project
// and serving inside metastore if any
class MetaManager(addrList: List[String]) {
  val cluster: TiCluster = getCluster()
  private val catalog: Catalog = cluster.getCatalog
  private val dbCache = HashMap[String, (TiDBInfo, Map[String, TiTableInfo])]()

  def getDatabases(update: Boolean = false): List[TiDBInfo] = {
    if (update) {
      loadDatabase
    }
    dbCache.values.map(_._1).toList
  }

  def getTables(db: TiDBInfo, update: Boolean = false): List[TiTableInfo] = {
    val dbNameL: String = db.getName.toLowerCase
    if (update) {
      loadTables(db)
    } else {
      dbCache.get(dbNameL).map(p => p._2.values.toList).getOrElse(List.empty[TiTableInfo])
    }
  }

  def loadTables(db: TiDBInfo): List[TiTableInfo] = {
    val dbNameL: String = db.getName.toLowerCase
    val tableMap: Map[String, TiTableInfo] = retrieveTableCache(db)
    dbCache.put(dbNameL, (db, retrieveTableCache(db)))
    tableMap.values.toList
  }

  def getTable(dbName: String, tableName: String): Option[TiTableInfo] = {
    val dbNameL = dbName.toLowerCase
    val tableNameL = tableName.toLowerCase

    if (dbCache.contains(dbNameL)) {
      val dbPair = dbCache(dbNameL)
      val db = dbPair._1
      var tableMap: Map[String, TiTableInfo] = dbPair._2
      if (tableMap.contains(tableNameL)) {
        tableMap.get(tableNameL)
      } else {
        tableMap = retrieveTableCache(db)
        dbCache.put(dbNameL, (db, tableMap))
        tableMap.get(tableNameL)
      }
    } else {
      Option.empty[TiTableInfo]
    }
  }

  def getDatabase(dbName: String): Option[TiDBInfo] = {
    val dbNameL = dbName.toLowerCase
    if (!dbCache.contains(dbNameL)) {
      loadDatabase
    }

    dbCache.get(dbNameL).map(p => p._1)
  }

  private def retrieveTableCache(db: TiDBInfo): Map[String, TiTableInfo] =
        catalog.listTables(db).map(table => (table.getName.toLowerCase, table))(breakOut)


  def loadDatabase: Unit = {
    catalog.listDatabases().foreach {
      db => {
        if (!dbCache.contains(db.getName.toLowerCase))
          dbCache.put(db.getName.toLowerCase, (db, Map[String, TiTableInfo]()))
      }
    }
  }

  private def getCluster(): TiCluster = {
    val conf = TiConfiguration.createDefault(addrList)
    TiCluster.getCluster(conf)
  }
}
