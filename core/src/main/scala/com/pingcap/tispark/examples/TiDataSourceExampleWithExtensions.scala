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
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiDataSourceExampleWithExtensions {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.tispark.write.enable", "true")
      .setIfMissing("spark.tispark.write.allow_spark_sql", "true")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("spark.tispark.tidb.addr", "tidb")
      .setIfMissing("spark.tispark.tidb.password", "")
      .setIfMissing("spark.tispark.tidb.port", "4000")
      .setIfMissing("spark.tispark.tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "127.0.0.1:2379")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    usingConfigInExtensions(sqlContext)

    usingConfigInDataSource(sqlContext)
  }

  def usingConfigInExtensions(sqlContext: SQLContext): Unit = {
    // use tidb config in spark config if does not provide in data source config
    val tidbOptions: Map[String, String] = Map()
    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
  }

  def usingConfigInDataSource(sqlContext: SQLContext): Unit = {
    // tidb config priority: data source config > spark config
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
      .filter("C_CUSTKEY = 1")
      .select("C_NAME")
    df.show()
  }
}
