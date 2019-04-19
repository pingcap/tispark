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
import org.apache.spark.sql.SparkSession

/**
 * before run the code in IDE, please enable maven profile `local-debug`
 */
object TiExtensionsExample {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setIfMissing("spark.app.name", getClass.getName)
      .setIfMissing("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      .setIfMissing("tidb.addr", "pd0")
      .setIfMissing("tidb.port", "4000")
      .setIfMissing("tidb.user", "root")
      .setIfMissing("spark.tispark.pd.addresses", "pd0:2379")
      .setIfMissing("spark.tispark.plan.allow_index_read", "true")

    val spark = SparkSession.builder.config(sparkConf).getOrCreate()

    val df = spark
      .sql(
        s"""
           |select *
           |from test.idx
           |where cid_id = "dedede" limit 6
         """.stripMargin
      )
    df.show()
    df.explain()
    /*
    spark
      .sql(
        s"""
           |select stat_date,device_id,city_id as ucity_id,cid_id,diary_service_id
           |from jerry_prod.data_feed_exposure
           |where cid_type = "diary" limit 6
         """.stripMargin
      )
      .show(6)
   */

  }

}
