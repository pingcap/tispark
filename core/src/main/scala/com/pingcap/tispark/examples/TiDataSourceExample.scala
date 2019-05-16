/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tispark.examples

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiDataSourceExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.tispark.write.enable", "true")
      .setIfMissing("spark.tispark.write.allow_spark_sql", "true")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    readUsingDataSourceAPI(sqlContext)

    pushDownUsingDataSourceAPI(sqlContext)

    writeUsingDataSourceAPI(sqlContext)

    readWithSparkSQLAPI(sqlContext)

    // TODO: to support
    //readWithSchemaUsingSparkSQLAPI(sqlContext)

    writeUsingSparkSQLAPI(sqlContext)
  }

  private def readUsingDataSourceAPI(sqlContext: SQLContext): DataFrame = {
    // Common options can also be passed in,
    // e.g. spark.tispark.plan.allow_agg_pushdown, spark.tispark.plan.allow_index_read, etc.
    // spark.tispark.plan.allow_index_read is optional
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379"
    )

    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()
    df.show()
    df
  }

  private def pushDownUsingDataSourceAPI(sqlContext: SQLContext): DataFrame = {
    // Common options can also be passed in,
    // e.g. spark.tispark.plan.allow_agg_pushdown, spark.tispark.plan.allow_index_read, etc.
    // spark.tispark.plan.allow_index_read is optional
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379",
      "spark.tispark.plan.allow_index_read" -> "true"
    )

    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
    df
  }

  private def writeUsingDataSourceAPI(sqlContext: SQLContext): Unit = {
    // Common options can also be passed in,
    // e.g. spark.tispark.plan.allow_agg_pushdown, spark.tispark.plan.allow_index_read, etc.
    // spark.tispark.plan.allow_index_read is optional
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379",
      "spark.tispark.plan.allow_index_read" -> "true"
    )

    val df = readUsingDataSourceAPI(sqlContext)

    // Append
    // target_table_append should exist in tidb
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "target_table_append")
      .mode("append")
      .save()
  }

  private def readWithSparkSQLAPI(sqlContext: SQLContext): Unit = {
    sqlContext.sql(s"""
                      |CREATE TABLE test1
                      |USING tidb
                      |OPTIONS (
                      |  database 'tpch_test',
                      |  table 'CUSTOMER',
                      |  tidb.addr 'tidb',
                      |  tidb.password '',
                      |  tidb.port '4000',
                      |  tidb.user 'root',
                      |  spark.tispark.pd.addresses '127.0.0.1:2379'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |select C_NAME from test1 where C_CUSTKEY = 1
       """.stripMargin).show()
  }

  private def readWithSchemaUsingSparkSQLAPI(sqlContext: SQLContext): Unit = {
    sqlContext.sql(s"""
                      |CREATE TABLE test2(
                      |  `C_CUSTKEY` integer,
                      |  `C_NAME` string,
                      |  `C_ADDRESS` string,
                      |  `C_NATIONKEY` integer,
                      |  `C_PHONE` string,
                      |  `C_ACCTBAL` double,
                      |  `C_MKTSEGMENT` string,
                      |  `C_COMMENT` string)
                      |USING tidb
                      |OPTIONS (
                      |  database 'tpch_test',
                      |  table 'CUSTOMER',
                      |  tidb.addr 'tidb',
                      |  tidb.password '',
                      |  tidb.port '4000',
                      |  tidb.user 'root',
                      |  spark.tispark.pd.addresses '127.0.0.1:2379'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |select C_NAME from test2 where C_CUSTKEY = 1
       """.stripMargin).show()
  }

  private def writeUsingSparkSQLAPI(sqlContext: SQLContext): Unit = {
    sqlContext.sql(s"""
                      |CREATE TABLE writeUsingSparkSQLAPI_src
                      |USING tidb
                      |OPTIONS (
                      |  database 'tpch_test',
                      |  table 'CUSTOMER',
                      |  tidb.addr 'tidb',
                      |  tidb.password '',
                      |  tidb.port '4000',
                      |  tidb.user 'root',
                      |  spark.tispark.pd.addresses '127.0.0.1:2379'
                      |)
       """.stripMargin)

    // target_table should exist in tidb
    sqlContext.sql(s"""
                      |CREATE TABLE writeUsingSparkSQLAPI_dest
                      |USING tidb
                      |OPTIONS (
                      |  database 'tpch_test',
                      |  table 'target_table',
                      |  tidb.addr 'tidb',
                      |  tidb.password '',
                      |  tidb.port '4000',
                      |  tidb.user 'root',
                      |  spark.tispark.pd.addresses '127.0.0.1:2379'
                      |)
       """.stripMargin)

    // insert into values
    sqlContext.sql("""
                     |insert into writeUsingSparkSQLAPI_dest values
                     |(1000,
                     |"Customer#000001000",
                     |"AnJ5lxtLjioClr2khl9pb8NLxG2",
                     |9,
                     |"19-407-425-2584",
                     |2209.81,
                     |"AUTOMOBILE",
                     |". even, express theodolites upo")
      """.stripMargin)

    // insert into select
    sqlContext.sql(s"""
                      |insert into writeUsingSparkSQLAPI_dest select * from writeUsingSparkSQLAPI_src
       """.stripMargin).show()
  }
}
