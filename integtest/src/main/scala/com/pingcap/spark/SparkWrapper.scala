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

import java.sql.Date

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.sql.types.{BinaryType, StructField}
import org.apache.spark.sql.{DataFrame, SparkSession, TiContext}

import scala.collection.mutable.ArrayBuffer


class SparkWrapper() extends LazyLogging {
  private val spark = SparkSession
    .builder()
    .appName("TiSpark Integration Test")
    .getOrCreate()
    .newSession()

  val ti = new TiContext(spark)

  def init(databaseName: String): Unit = {
    logger.info("Mapping database: " + databaseName)
    ti.tidbMapDatabase(databaseName)
  }

  def toOutput(value: Any, colType: String): Any = {
    value match {
      case _: Array[Byte] =>
        var str: String = new String
        for (b <- value.asInstanceOf[Array[Byte]]) {
          str = str.concat(b.toString)
        }
        str
      case _: BigDecimal =>
        value.asInstanceOf[BigDecimal].setScale(2, BigDecimal.RoundingMode.HALF_UP)
      case _: Date if colType.equalsIgnoreCase("YEAR") =>
        value.toString.split("-")(0)
      case default =>
        default
    }
  }

  def dfData(df: DataFrame, schema: scala.Array[StructField]): List[List[Any]] =
    df.collect().map(row => {
      val rowRes = ArrayBuffer.empty[Any]
      for (i <- 0 until row.length) {
        if (schema(i).dataType.isInstanceOf[BinaryType]) {
          rowRes += new String(row.get(i).asInstanceOf[Array[Byte]])
        } else {
          rowRes += toOutput(row.get(i), schema(i).dataType.typeName)
        }
      }
      rowRes.toList
    }).toList

  def querySpark(sql: String): List[List[Any]] = {
    logger.info("Running query on spark: " + sql)
    val df = spark.sql(sql)
    val schema = df.schema.fields

    dfData(df, schema)
  }
}
