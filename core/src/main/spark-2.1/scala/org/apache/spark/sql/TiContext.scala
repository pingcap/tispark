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
import org.json4s.DefaultFormats
import org.json4s.JsonAST._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods
import org.json4s.jackson.JsonMethods._

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
    implicit val formats = DefaultFormats

    def getRegionDistribution(dbName: String, tableName: String): Map[String, Integer] =
      RegionUtils.getRegionDistribution(tiSession, dbName, tableName).asScala.toMap

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
          val json: JValue = JsonMethods.parse(resStr.body)
          val leader = (json \ "leader").extract[JObject]
          val peers = (json \ "peers").extract[JArray].arr
          val leaderStoreId = (leader \ "store_id").extract[Long]

          val targetLeaders = peers
            .map(x => (x \ "store_id").extract[Long])
            .filterNot(_ == leaderStoreId)
            .filter(
              id =>
                storeRegionCount.contains(id) &&
                  storeRegionCount(id) < storeRegionCount(leaderStoreId) &&
                  storeRegionCount(id) < avgRegionCount
            )

          if (targetLeaders.nonEmpty && transCount < maxTrans) {
            val toStore = targetLeaders.minBy(storeRegionCount(_))
            val req = ("name" -> "transfer-leader") ~ ("region_id" -> JDecimal(
              BigDecimal(regionId)
            )) ~ ("to_store_id" -> JDecimal(BigDecimal(toStore)))
            val resp = Http(s"$pdAddr/$operatorsPrefix")
              .postData(compact(render(req)))
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

  def getDataFrame(dbName: String, tableName: String): DataFrame = {
    val tiRelation = new TiDBRelation(
      tiSession,
      new TiTableReference(dbName, tableName),
      meta
    )(sqlContext)
    sqlContext.baseRelationToDataFrame(tiRelation)
  }

  // add backtick for table name in case it contains, e.g., a minus sign
  private def getViewName(dbName: String, tableName: String, dbNameAsPrefix: Boolean): String =
    "`" + (if (dbNameAsPrefix) dbName + "_" + tableName else tableName) + "`"

  // tidbMapTable does not do any check any meta information
  // it just register table for later use
  def tidbMapTable(dbName: String,
                   tableName: String,
                   dbNameAsPrefix: Boolean = false): DataFrame = {
    val df = getDataFrame(dbName, tableName)
    val viewName = getViewName(dbName, tableName, dbNameAsPrefix)
    df.createOrReplaceTempView(viewName)
    logInfo("Registered table [" + tableName + "] as [" + viewName + "]")
    df
  }

  def tidbMapDatabase(dbName: String, dbNameAsPrefix: Boolean): Unit =
    tidbMapDatabase(dbName, dbNameAsPrefix, autoLoad)

  def tidbMapDatabase(dbName: String,
                      dbNameAsPrefix: Boolean = false,
                      autoLoadStatistics: Boolean = autoLoad): Unit =
    for {
      db <- meta.getDatabase(dbName)
      table <- meta.getTables(db)
    } {
      var sizeInBytes = Long.MaxValue
      val tableName = table.getName
      if (autoLoadStatistics) {
        statisticsManager.loadStatisticsInfo(table)
      }
      sizeInBytes = statisticsManager.estimateTableSize(table)

      if (!sqlContext.sparkSession.catalog.tableExists(tableName)) {
        val rel: TiDBRelation = new TiDBRelation(
          tiSession,
          new TiTableReference(dbName, tableName, sizeInBytes),
          meta
        )(sqlContext)

        val viewName = getViewName(dbName, tableName, dbNameAsPrefix)
        sqlContext.baseRelationToDataFrame(rel).createTempView(viewName)
        logInfo("Registered table [" + tableName + "] as [" + viewName + "]")
      } else {
        logInfo(
          "Duplicate table [" + tableName + "] exist in catalog, you might want to set dbNameAsPrefix = true"
        )
      }
    }
}
