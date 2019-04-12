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

import com.pingcap.tispark.TiUtils.TIDB_SOURCE_NAME
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiDataSourceExampleWithoutExtensions {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    dbScan(sqlContext)

    pushDown(sqlContext)

    batchWrite(sqlContext)

    readUsingPureSQL(sqlContext)

    //readWithSchemaUsingPureSQL(sqlContext)
  }

  private def dbScan(sqlContext: SQLContext): DataFrame = {
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379",
      "spark.tispark.plan.allow_index_read" -> "true"
    )

    val df = sqlContext.read
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", "tpch_test.CUSTOMER")
      .load()
    df.show()
    df
  }

  private def pushDown(sqlContext: SQLContext): DataFrame = {
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379",
      "spark.tispark.plan.allow_index_read" -> "true"
    )

    val df = sqlContext.read
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", "tpch_test.CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
    df
  }

  private def batchWrite(sqlContext: SQLContext): Unit = {
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379",
      "spark.tispark.plan.allow_index_read" -> "true"
    )

    val df = dbScan(sqlContext)
    df.write
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", "tpch_test.DATASOURCE_API_CUSTOMER2")
      .mode(SaveMode.Overwrite)
      .save()
  }

  private def readUsingPureSQL(sqlContext: SQLContext): Unit = {
    sqlContext.sql(s"""
                      |CREATE TABLE test1
                      |USING com.pingcap.tispark
                      |OPTIONS (
                      |  dbtable 'tpch_test.CUSTOMER',
                      |  tidb.addr '127.0.0.1',
                      |  tidb.password '',
                      |  tidb.port '4000',
                      |  tidb.user 'root',
                      |  spark.tispark.pd.addresses '127.0.0.1:2379',
                      |  spark.tispark.plan.allow_index_read 'true'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |select C_NAME from test1 where C_CUSTKEY = 1
       """.stripMargin).show()
  }

  private def readWithSchemaUsingPureSQL(sqlContext: SQLContext): Unit = {
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
                      |USING com.pingcap.tispark
                      |OPTIONS (
                      |  dbtable 'tpch_test.CUSTOMER',
                      |  tidb.addr '127.0.0.1',
                      |  tidb.password '',
                      |  tidb.port '4000',
                      |  tidb.user 'root',
                      |  spark.tispark.pd.addresses '127.0.0.1:2379',
                      |  spark.tispark.plan.allow_index_read 'true'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |select C_NAME from test2 where C_CUSTKEY = 1
       """.stripMargin).show()
  }
}
