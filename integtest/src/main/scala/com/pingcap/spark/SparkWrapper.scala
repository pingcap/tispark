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

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.{SparkSession, TiContext}
import com.pingcap.spark.Utils._

import scala.collection.mutable.ArrayBuffer


class SparkWrapper(prop: Properties) extends LazyLogging {
  private val KeyPDAddress = "pd.addrs"

  private val spark = SparkSession
    .builder()
    .appName("TiSpark Integration Test")
    .getOrCreate()

  val ti = new TiContext(spark, getOrThrow(prop, KeyPDAddress).split(",").toList)

  def init(databaseName: String): Unit = {
    logger.info("Mapping database: " + databaseName)
    ti.tidbMapDatabase(databaseName)
  }

  def querySpark(sql: String): List[List[Any]] = {
    logger.info("Running query on spark: " + sql)
    spark.sql(sql).collect().map(row => {
      val rowRes = ArrayBuffer.empty[Any]
      for (i <- 0 to row.length - 1) {
        rowRes += row.get(i)
      }
      rowRes.toList
    }).toList
  }
}
