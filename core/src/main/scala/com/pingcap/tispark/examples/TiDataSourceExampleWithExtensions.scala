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
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import com.pingcap.tispark.TiUtils.TIDB_SOURCE_NAME

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiDataSourceExampleWithExtensions {

  private val tidbUser = "root"
  private val tidbPassword = ""
  private val tidbAddr = "127.0.0.1"
  private val tidbPort = 4000
  private val pdAddresses = "127.0.0.1:2379"

  var sqlContext: SQLContext = _

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", tidbAddr)
      .setIfMissing("tidb.password", tidbPassword)
      .setIfMissing("tidb.port", s"$tidbPort")
      .setIfMissing("tidb.user", tidbUser)
      .setIfMissing("spark.tispark.pd.addresses", pdAddresses)
      .setIfMissing("spark.tispark.plan.allow_index_read", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    sqlContext = spark.sqlContext

    dbScan()

    pushDown()

    batchWrite()

    readUsingPureSQL()

    //readWithSchemaUsingPureSQL()
  }

  def dbScan(): DataFrame = {
    val tidbOptions: Map[String, String] = Map()
    val df = sqlContext.read
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", "tpch_test.CUSTOMER")
      .load()
    df.show()
    df
  }

  def pushDown(): DataFrame = {
    val tidbOptions: Map[String, String] = Map()
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

  def batchWrite(): Unit = {
    val tidbOptions: Map[String, String] = Map()
    val df = dbScan()
    df.write
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", "tpch_test.DATASOURCE_API_CUSTOMER3")
      .mode(SaveMode.Overwrite)
      .save()
  }

  def readUsingPureSQL(): Unit = {
    sqlContext.sql(s"""
                      |CREATE TABLE test1
                      |USING com.pingcap.tispark
                      |OPTIONS (
                      |  dbtable 'tpch_test.CUSTOMER'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |select C_NAME from test1 where C_CUSTKEY = 1
       """.stripMargin).show()
  }

  def readWithSchemaUsingPureSQL(): Unit = {
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
                      |  dbtable 'tpch_test.CUSTOMER'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |select C_NAME from test2 where C_CUSTKEY = 1
       """.stripMargin).show()
  }
}
