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

import scala.collection.mutable
import scala.collection.JavaConversions._


object MetaManager {
  private val clusterCache = mutable.HashMap[String, TiCluster]()

  def resolveTable(options: TiOptions): TiTableInfo = {
    val cluster: TiCluster = getCluster(options)

    val catalog: Catalog = cluster.getCatalog
    val database: TiDBInfo = catalog.getDatabase(options.databaseName)

    catalog.getTable(database, options.tableName)
  }

  def getCluster(options: TiOptions): TiCluster = {
    val addrList: List[String] = options.addresses

    addrList.find(clusterCache.contains).map(clusterCache)
        .getOrElse(getClusterByAddresses(addrList))
  }

  private def getClusterByAddresses(addrList: List[String]): TiCluster = {
    val conf = TiConfiguration.createDefault(addrList)
    TiCluster.getCluster(conf)
  }
}
