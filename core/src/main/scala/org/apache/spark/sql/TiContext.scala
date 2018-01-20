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

package org.apache.spark.sql

import java.{lang, util}

import com.pingcap.tikv.tools.RegionUtils
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import play.api.libs.json._

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import scala.collection.mutable
import scalaj.http.Http

class TiContext(val session: SparkSession) extends Serializable with Logging {
  val sqlContext: SQLContext = session.sqlContext
  val conf: SparkConf = session.sparkContext.conf
  val tiConf: TiConfiguration = TiUtils.sparkConfToTiConf(conf)
  val tiSession: TiSession = TiSession.create(tiConf)
  val meta: MetaManager = new MetaManager(tiSession.getCatalog)

  val debug: DebugTool = new DebugTool

  TiUtils.sessionInitialize(session, tiSession)

  final val version: String = TiSparkVersion.version

  class DebugTool {
    def getRegionDistribution(dbName: String, tableName: String): Map[String, Integer] = {
      RegionUtils.getRegionDistribution(tiSession, dbName, tableName).asScala.toMap
    }

    def balanceRegionByTable(pdAddr: String,
                             dbName: String,
                             tableName: String): Map[String, Integer] = {
      val regionsPrefix = "pd/api/v1/regions"
      val regionsWriteflowPrefix = "pd/api/v1/regions/writeflow"
      val regionsReadflowPrefix = "pd/api/v1/regions/readflow"
      val regionIDPrefix = "pd/api/v1/region/id"
      val regionKeyPrefix = "pd/api/v1/region/key"
      val storeRegionId = RegionUtils.getStoreRegionIdDistribution(tiSession, dbName, tableName)
      val storeRegionCount = mutable.Map[Long, Long]()

      storeRegionId.asScala.foreach((tuple: (lang.Long, java.util.List[lang.Long])) => {
        storeRegionCount(tuple._1) = tuple._2.size()
      })

      storeRegionId.asScala
        .flatMap(_._2)
        .foreach((regionId: lang.Long) => {
          val resStr = Http(s"$pdAddr/$regionIDPrefix/$regionId").asString
          val json: JsValue = Json.parse(resStr.body)
          val leader = json("leader").as[JsObject]
          val peers = json("peers").as[JsArray].value
          val leaderStoreId = leader("store_id").as[JsNumber].value.toLong

          val targetLeaders = peers
            .map { _("store_id").as[JsNumber].value.toLong }
            .filterNot { _ == leaderStoreId }
            .filter { id =>
              storeRegionCount.contains(id) &&
              storeRegionCount(id) + 10 < storeRegionCount(leaderStoreId)
            }

          if (targetLeaders.nonEmpty) {
            println(
              s"Transfer $regionId leader :Store $leaderStoreId to Store ${targetLeaders.head}"
            )
          }
        })
      getRegionDistribution(dbName, tableName)
    }
  }

  def tidbTable(dbName: String, tableName: String): DataFrame = {
    val tiRelation = new TiDBRelation(
      tiSession,
      new TiTableReference(dbName, tableName),
      meta
    )(sqlContext)
    sqlContext.baseRelationToDataFrame(tiRelation)
  }

  def tidbMapDatabase(dbName: String, dbNameAsPrefix: Boolean = false): Unit =
    for {
      db <- meta.getDatabase(dbName)
      table <- meta.getTables(db)
    } {
      val rel: TiDBRelation = new TiDBRelation(
        tiSession,
        new TiTableReference(dbName, table.getName),
        meta
      )(sqlContext)

      if (!sqlContext.sparkSession.catalog.tableExists(table.getName)) {
        val tableName = if (dbNameAsPrefix) db.getName + "_" + table.getName else table.getName
        sqlContext.baseRelationToDataFrame(rel).createTempView(tableName)
        logInfo("Registered table " + table.getName)
      }
    }
}
