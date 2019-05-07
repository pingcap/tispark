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

import com.pingcap.tispark.{TiBatchWrite, TiDBOptions, TiTableReference}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, TiContext}

object TiBatchWritePressureTest {
  def main(args: Array[String]) = {

    if (args.length < 4) {
      throw new Exception("wrong arguments!")
    }

    // TPCH_001 ORDERS batchwrite ORDERS_001
    val inputDatabase = args(0)
    val inputTable = args(1)
    val outputDatabase = args(2)
    val outputTable = args(3)

    val regionSplitNumber = if (args.length >= 5) Some(args(4).toInt) else None
    val enableRegionPreSplit = if (args.length >= 6) args(5).toBoolean else false

    // init
    val start = System.currentTimeMillis()
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "localhost")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "localhost:2379")
//      .setIfMissing("spark.tispark.show_rowid", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val ti = new TiContext(spark)

    // select
    spark.sql("show databases").show()

    spark.sql(s"use $inputDatabase")
    val df = spark.sql(s"select * from $inputTable")

    // batch write
    val options = new TiDBOptions(
      sparkConf.getAll.toMap ++ Map("database" -> outputDatabase, "table" -> outputTable)
    )
    TiBatchWrite.writeToTiDB(df.rdd, ti, options, regionSplitNumber, enableRegionPreSplit)

    // time
    val end = System.currentTimeMillis()
    val seconds = (end - start) / 1000
    println(s"total time: $seconds seconds")
  }
}
