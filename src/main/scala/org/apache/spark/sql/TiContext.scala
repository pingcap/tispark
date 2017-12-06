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

import java.util.function

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._
import com.pingcap.tikv.event.CacheInvalidateEvent
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tispark._
import com.pingcap.tispark.accumulator.{AccumulatorManager, CacheInvalidateAccumulator}
import com.pingcap.tispark.handler.CacheInvalidateEventHandler
import com.pingcap.tispark.listener.PDCacheInvalidateListener
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, SparkContext}

class TiContext(val session: SparkSession) extends Serializable with Logging {
  val sqlContext: SQLContext = session.sqlContext
  val conf: SparkConf = session.sparkContext.conf
  val tiConf: TiConfiguration = TiUtils.sparkConfToTiConf(conf)
  val sparkContext: SparkContext = session.sparkContext

  val tiSession: TiSession = TiSession.create(tiConf, AccumulatorManager.CACHE_ACCUMULATOR_FUNCTION)
  val meta: MetaManager = new MetaManager(tiSession.getCatalog)

  val cacheListener =
    new PDCacheInvalidateListener(
      AccumulatorManager.CACHE_INVALIDATE_ACCUMULATOR,
      CacheInvalidateEventHandler(tiSession.getRegionManager)
    )

  sparkContext.addSparkListener(cacheListener)
  sparkContext.register(
    AccumulatorManager.CACHE_INVALIDATE_ACCUMULATOR,
    AccumulatorManager.ACCUMULATOR_NAME
  )
  TiUtils.sessionInitialize(session)

  final val version: String = TiSparkVersion.version

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
