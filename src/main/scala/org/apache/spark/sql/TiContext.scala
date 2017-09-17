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

import com.pingcap.tispark.{MetaManager, TiDBRelation, TiOptions}
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions


class TiContext (val session: SparkSession, addressList: List[String]) extends Serializable with Logging {
  val sqlContext: SQLContext = session.sqlContext
  val meta: MetaManager = new MetaManager(addressList)

  session.experimental.extraStrategies ++= Seq(new TiStrategy(sqlContext))

  def this (session: SparkSession, addressList: java.util.List[String]) {
    this(session, JavaConversions.asScalaBuffer(addressList).toList)
  }

  def tidbTable(dbName: String,
                tableName: String): DataFrame = {
    val tiRelation = new TiDBRelation(new TiOptions(addressList, dbName, tableName), meta)(sqlContext)
    sqlContext.baseRelationToDataFrame(tiRelation)
  }

  def tidbMapDatabase(dbName: String, dbNameAsPrefix: Boolean = false): Unit =
    meta.getDatabase(dbName).foreach {
      db => {
        meta.getTables(db).foreach {
          table => {
            val rel: TiDBRelation =
              new TiDBRelation(new TiOptions(addressList, dbName, table.getName), meta)(sqlContext)
            if (!sqlContext.sparkSession.catalog.tableExists(table.getName)) {
              val tableName = if (dbNameAsPrefix) db.getName + "_" + table.getName else table.getName
              sqlContext.baseRelationToDataFrame(rel).createTempView(tableName)
              logInfo("Registered table " + table.getName)
            }
          }
        }
      }
    }
}
