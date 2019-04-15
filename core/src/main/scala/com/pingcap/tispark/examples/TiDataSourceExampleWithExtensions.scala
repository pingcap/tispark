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

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiDataSourceExampleWithExtensions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "127.0.0.1")
      .setIfMissing("tidb.password", "")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "127.0.0.1:2379")
      .setIfMissing("spark.tispark.plan.allow_index_read", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    usingConfigInExtensions(sqlContext)

    usingConfigInDataSource(sqlContext)
  }

  def usingConfigInExtensions(sqlContext: SQLContext): DataFrame = {
    // use tidb config in spark config if does not provide in data source config
    val tidbOptions: Map[String, String] = Map()
    val df = sqlContext.read
      .format("com.pingcap.tispark")
      .options(tidbOptions)
      .option("dbtable", "tpch_test.CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
    df
  }

  def usingConfigInDataSource(sqlContext: SQLContext): DataFrame = {
    // tidb config priority: data source config > spark config
    val tidbOptions: Map[String, String] = Map(
      "tidb.addr" -> "127.0.0.1",
      "tidb.password" -> "",
      "tidb.port" -> "4000",
      "tidb.user" -> "root",
      "spark.tispark.pd.addresses" -> "127.0.0.1:2379",
      "spark.tispark.plan.allow_index_read" -> "true"
    )

    val df = sqlContext.read
      .format("com.pingcap.tispark")
      .options(tidbOptions)
      .option("dbtable", "tpch_test.CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
    df
  }
}
