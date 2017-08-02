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

import org.apache.spark.sql.{SparkSession, TiContext}

import scala.collection.mutable.ArrayBuffer


class SparkWrapper(prop: Properties) {
  private val spark = SparkSession
    .builder()
    .appName("TiSpark Integration Test")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")
  val ti = new TiContext(spark, List(prop.getProperty("pdaddr")))


  def querySpark(sql: String): List[List[Any]] = {
    spark.sql(sql).collect().map(row => {
      val rowRes = ArrayBuffer.empty[Any]
      for (i <- 0 to row.length - 1) {
        rowRes += row.get(i)
      }
      rowRes.toList
    }).toList
  }
}
