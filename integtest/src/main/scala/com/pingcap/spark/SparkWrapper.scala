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

import java.sql.Types

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.sql.{SparkSession, TiContext}
import org.apache.spark.unsafe.types.ByteArray

import scala.collection.mutable.ArrayBuffer


class SparkWrapper() extends LazyLogging {
  private val spark = SparkSession
    .builder()
    .appName("TiSpark Integration Test")
    .getOrCreate()

  val ti = new TiContext(spark)

  def init(databaseName: String): Unit = {
    logger.info("Mapping database: " + databaseName)
    ti.tidbMapDatabase(databaseName)
  }

  def toOutput(value: Any): Any = {
    value match {
      case _: Array[Byte] =>
        var str: String = new String
        for (b <- value.asInstanceOf[Array[Byte]]) {
          str = str.concat(b.toString)
        }
        str
      case default => default
    }
  }

  def querySpark(sql: String): List[List[Any]] = {
    logger.info("Running query on spark: " + sql)
    val df = spark.sql(sql)
    val schema = df.schema.fields

    df.collect().map(row => {
      val rowRes = ArrayBuffer.empty[Any]
      for (i <- 0 until row.length) {
        rowRes += toOutput(row.get(i))
      }
      rowRes.toList
    }).toList
  }
}
