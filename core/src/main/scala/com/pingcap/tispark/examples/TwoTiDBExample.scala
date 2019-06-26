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
object TwoTiDBExample {

  private val srcTidbOptions: Map[String, String] = Map(
    "tidb.addr" -> "127.0.0.1",
    "tidb.password" -> "",
    "tidb.port" -> "4000",
    "tidb.user" -> "root",
    "spark.tispark.pd.addresses" -> "127.0.0.1:2379"
  )

  private val destTidbOptions: Map[String, String] = Map(
    "tidb.addr" -> "127.0.0.1",
    "tidb.password" -> "",
    "tidb.port" -> "14000",
    "tidb.user" -> "root",
    "spark.tispark.pd.addresses" -> "127.0.0.1:12379"
  )

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.tispark.write.enable", "true")
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sqlContext = spark.sqlContext

    read_write(sqlContext)

    read_read(sqlContext)

    write_write(sqlContext)

  }

  private def read_write(sqlContext: SQLContext): Unit = {
    // Read from src
    val df = sqlContext.read
      .format("tidb")
      .options(srcTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()

    // Upsert to dest
    df.write
      .format("tidb")
      .options(destTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER_DEST")
      .mode("append")
      .save()
  }

  private def read_read(sqlContext: SQLContext): Unit = {
    // Read from src
    sqlContext.read
      .format("tidb")
      .options(srcTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()
      .show(false)

    // Read from dest
    sqlContext.read
      .format("tidb")
      .options(destTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER_DEST")
      .load()
      .show(false)
  }

  private def write_write(sqlContext: SQLContext): Unit = {
    // Read from src
    val df = sqlContext.read
      .format("tidb")
      .options(srcTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER")
      .load()

    // Upsert to src
    df.write
      .format("tidb")
      .options(srcTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER_SRC")
      .mode("append")
      .save()

    // Upsert to dest
    df.write
      .format("tidb")
      .options(destTidbOptions)
      .option("database", "tpch_test")
      .option("table", "CUSTOMER_DEST2")
      .mode("append")
      .save()
  }
}
