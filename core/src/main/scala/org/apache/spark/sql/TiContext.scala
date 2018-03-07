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

import java.lang

import com.pingcap.tikv.tools.RegionUtils
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark._
import com.pingcap.tispark.statistics.StatisticsManager
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
  val statisticsManager: StatisticsManager = StatisticsManager.getInstance()
  private val autoLoad =
    conf.getBoolean("spark.tispark.statistics.auto_load", defaultValue = true)

  class DebugTool {
    def getRegionDistribution(dbName: String, tableName: String): Map[String, Integer] = {
      RegionUtils.getRegionDistribution(tiSession, dbName, tableName).asScala.toMap
    }

    /**
     * Balance region leaders of a single table.
     *
     * e.g.
     * `balanceRegionByTable("http://172.16.20.3:2379", "tpch_idx", "lineitem", 20)`
     * This method call will try to balance table `lineitem`'s leader distribution by
     * transforming those leaders reside in a single heavily used TiKV to other TiKVs.
     *
     * @param pdAddr    The PD address
     * @param dbName    Database name
     * @param tableName Table name
     * @param maxTrans  Maximum number of transformations this function can perform
     * @return The re-distributed information of original table
     */
    def balanceRegionByTable(pdAddr: String,
                             dbName: String,
                             tableName: String,
                             maxTrans: Int = 50): Map[String, Integer] = {
      val regionIDPrefix = "pd/api/v1/region/id"
      val operatorsPrefix = "pd/api/v1/operators"
      val storeRegionId = RegionUtils.getStoreRegionIdDistribution(tiSession, dbName, tableName)
      val storeRegionCount = mutable.Map[Long, Long]()

      storeRegionId.asScala.foreach((tuple: (lang.Long, java.util.List[lang.Long])) => {
        storeRegionCount(tuple._1) = tuple._2.size()
      })

      val avgRegionCount = storeRegionCount.values.sum / storeRegionCount.size

      var transCount = 0
      storeRegionId.asScala
        .flatMap(_._2)
        .foreach((regionId: lang.Long) => {
          val resStr = Http(s"$pdAddr/$regionIDPrefix/$regionId").asString
          val json: JsValue = Json.parse(resStr.body)
          val leader = json("leader").as[JsObject]
          val peers = json("peers").as[JsArray].value
          val leaderStoreId = leader("store_id").as[JsNumber].value.toLong

          val targetLeaders = peers
            .map(_("store_id").as[JsNumber].value.toLong)
            .filterNot(_ == leaderStoreId)
            .filter(
              id =>
                storeRegionCount.contains(id) &&
                  storeRegionCount(id) < storeRegionCount(leaderStoreId) &&
                  storeRegionCount(id) < avgRegionCount
            )

          if (targetLeaders.nonEmpty && transCount < maxTrans) {
            val toStore = targetLeaders.minBy(storeRegionCount(_))
            val req = Json.obj(
              "name" -> "transfer-leader",
              "region_id" -> JsNumber(BigDecimal(regionId)),
              "to_store_id" -> JsNumber(BigDecimal(toStore))
            )
            val resp = Http(s"$pdAddr/$operatorsPrefix")
              .postData(req.toString())
              .header("content-type", "application/json")
              .asString
            if (resp.isSuccess) {
              logInfo(
                s"Transfer $regionId leader :Store $leaderStoreId to Store $toStore successfully"
              )
              storeRegionCount(leaderStoreId) -= 1
              storeRegionCount(toStore) += 1
            } else {
              logError(
                s"Transfer $regionId leader :Store $leaderStoreId to Store $toStore failed -- ${resp.body}"
              )
            }
            transCount += 1
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

  def tidbMapDatabase(dbName: String,
                      dbNameAsPrefix: Boolean = false,
                      autoLoadStatistics: Boolean = autoLoad): Unit =
    for {
      db <- meta.getDatabase(dbName)
      table <- meta.getTables(db)
    } {
      var sizeInBytes = Long.MaxValue
      if (autoLoadStatistics) {
        statisticsManager.loadStatisticsInfo(table)
        val count = statisticsManager.getTableCount(table.getId)
        if (count == 0) sizeInBytes = 0
        else if (Long.MaxValue / count > 64) sizeInBytes = 64 * count
      }

      val rel: TiDBRelation = new TiDBRelation(
        tiSession,
        new TiTableReference(dbName, table.getName, sizeInBytes),
        meta
      )(sqlContext)

      if (!sqlContext.sparkSession.catalog.tableExists(table.getName)) {
        val tableName = if (dbNameAsPrefix) db.getName + "_" + table.getName else table.getName
        sqlContext.baseRelationToDataFrame(rel).createTempView(tableName)
        logInfo("Registered table " + table.getName)
      }
    }
}
