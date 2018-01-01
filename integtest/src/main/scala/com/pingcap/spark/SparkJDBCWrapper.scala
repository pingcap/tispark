/*
 *
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
 *
 */

package com.pingcap.spark

import java.util.Properties

import com.pingcap.spark.Utils.getOrThrow
import org.apache.spark.sql.{Row, SparkSession}

class SparkJDBCWrapper(prop: Properties) extends SparkWrapper {
  private val spark_jdbc = SparkSession
    .builder()
    .appName("TiSpark Integration Test SparkJDBCWrapper")
    .getOrCreate()
    .newSession()

  private val KeyTiDBAddress = "tidb.addr"
  private val KeyTiDBPort = "tidb.port"
  private val KeyTiDBUser = "tidb.user"

  private val jdbcUsername = getOrThrow(prop, KeyTiDBUser)
  private val jdbcHostname = getOrThrow(prop, KeyTiDBAddress)
  private val jdbcPort = Integer.parseInt(getOrThrow(prop, KeyTiDBPort))
  private val jdbcUrl = s"jdbc:mysql://$jdbcHostname:$jdbcPort?user=$jdbcUsername"
  private var dbName = ""
  private var tableNames: Array[String] = _

  override def init(databaseName: String): Unit = {
    try {
      val tableDF = spark_jdbc.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "information_schema.tables")
        .option("user", jdbcUsername)
        .option("driver", "com.mysql.jdbc.Driver")
        .load()
        .filter(s"table_schema = '$databaseName'")
        .select("TABLE_NAME")
      tableNames = tableDF.collect().map((row: Row) => row.get(0).toString)
      tableNames.foreach(createTempView(databaseName, _))
      dbName = databaseName
    } catch {
      case e: Exception =>
        logger.error(s"Error when fetching jdbc data using $jdbcUrl: " + e.getMessage)
    }
  }

  def createTempView(dbName: String, viewName: String): Unit = {
    spark_jdbc.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$dbName.$viewName")
      .option("user", jdbcUsername)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
      .createOrReplaceTempView(viewName)
  }

  override def querySpark(sql: String): List[List[Any]] = {
    logger.info("Running query on spark-jdbc: " + sql)
    val df = spark_jdbc.sql(sql)
    val schema = df.schema.fields

    dfData(df, schema)
  }

  override def close(): Unit = {
    spark_jdbc.close()
  }
}
