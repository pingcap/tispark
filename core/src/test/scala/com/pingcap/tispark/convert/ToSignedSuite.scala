/*
 * Copyright 2020 PingCAP, Inc.
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

package com.pingcap.tispark.convert

import com.pingcap.tispark.datasource.BaseBatchWriteTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * SINGED type include:
 * 1. TINYINT SINGED
 * 2. SMALLINT SINGED
 * 3. MEDIUMINT SINGED
 * 4. INT SINGED
 * 5. BIGINT SINGED
 * 6. BOOLEAN
 */
class ToSignedSuite extends BaseBatchWriteTest("test_data_type_convert_to_signed") {

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", LongType),
      StructField("c2", LongType),
      StructField("c3", LongType),
      StructField("c4", LongType),
      StructField("c5", LongType),
      StructField("c6", LongType)))

  test("Test Convert from java.lang.Boolean to SINGED") {
    // success
    // java.lang.Boolean -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, null, true, true, true, true, true)
        val row3 = Row(3, false, null, false, false, false, false)
        val row4 = Row(4, true, false, null, false, true, true)
        val row5 = Row(5, true, false, false, null, true, false)
        val row6 = Row(6, true, false, true, false, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType),
            StructField("c3", BooleanType),
            StructField("c4", BooleanType),
            StructField("c5", BooleanType),
            StructField("c6", BooleanType)))

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, null, 1L, 1L, 1L, 1L, 1L)
        val readRow3 = Row(3, 0L, null, 0L, 0L, 0L, 0L)
        val readRow4 = Row(4, 1L, 0L, null, 0L, 1L, 1L)
        val readRow5 = Row(5, 1L, 0L, 0L, null, 1L, 0L)
        val readRow6 = Row(6, 1L, 0L, 1L, 0L, null, null)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Byte to SIGNED") {
    // success
    // java.lang.Byte -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val zero: java.lang.Byte = java.lang.Byte.valueOf("0")
        val one: java.lang.Byte = java.lang.Byte.valueOf("1")
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.MAX_VALUE
        val c: java.lang.Byte = java.lang.Byte.valueOf("-11")
        val d: java.lang.Byte = java.lang.Byte.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, null, a, b, c, d, zero)
        val row3 = Row(3, b, null, d, a, a, one)
        val row4 = Row(4, c, c, null, a, d, zero)
        val row5 = Row(5, b, b, b, null, a, one)
        val row6 = Row(6, c, c, a, d, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ByteType),
            StructField("c2", ByteType),
            StructField("c3", ByteType),
            StructField("c4", ByteType),
            StructField("c5", ByteType),
            StructField("c6", ByteType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Short to SIGNED") {
    // success
    // java.lang.Short -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Short = java.lang.Short.valueOf("1")
        val zero: java.lang.Short = java.lang.Short.valueOf("0")
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("-11")

        val maxByte: java.lang.Short = java.lang.Byte.MAX_VALUE.toShort
        val minByte: java.lang.Short = java.lang.Byte.MIN_VALUE.toShort
        val maxShort: java.lang.Short = java.lang.Short.MAX_VALUE
        val minShort: java.lang.Short = java.lang.Short.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxShort, maxShort, one)
        val row3 = Row(3, minByte, minShort, minShort, minShort, minShort, zero)
        val row4 = Row(4, null, b, null, a, a, one)
        val row5 = Row(5, b, b, b, a, a, zero)
        val row6 = Row(6, b, b, null, a, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ShortType),
            StructField("c2", ShortType),
            StructField("c3", ShortType),
            StructField("c4", ShortType),
            StructField("c5", ShortType),
            StructField("c6", ShortType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Integer to SIGNED") {
    // success
    // java.lang.Integer -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Integer = java.lang.Integer.valueOf("1")
        val zero: java.lang.Integer = java.lang.Integer.valueOf("0")
        val a: java.lang.Integer = java.lang.Integer.valueOf("11")
        val b: java.lang.Integer = java.lang.Integer.valueOf("-11")

        val maxByte: java.lang.Integer = java.lang.Byte.MAX_VALUE.toInt
        val minByte: java.lang.Integer = java.lang.Byte.MIN_VALUE.toInt
        val maxShort: java.lang.Integer = java.lang.Short.MAX_VALUE.toInt
        val minShort: java.lang.Integer = java.lang.Short.MIN_VALUE.toInt
        val maxInteger: java.lang.Integer = java.lang.Integer.MAX_VALUE
        val minInteger: java.lang.Integer = java.lang.Integer.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger, one)
        val row3 = Row(3, minByte, minShort, minShort, minInteger, minInteger, zero)
        val row4 = Row(4, null, b, null, a, a, one)
        val row5 = Row(5, b, b, b, a, a, zero)
        val row6 = Row(6, b, b, null, a, null, one)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", IntegerType),
            StructField("c2", IntegerType),
            StructField("c3", IntegerType),
            StructField("c4", IntegerType),
            StructField("c5", IntegerType),
            StructField("c6", IntegerType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Long to SIGNED") {
    // success
    // java.lang.Long -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Long = java.lang.Long.valueOf("1")
        val zero: java.lang.Long = java.lang.Long.valueOf("0")
        val a: java.lang.Long = java.lang.Long.valueOf("11")
        val b: java.lang.Long = java.lang.Long.valueOf("-11")

        val maxByte: java.lang.Long = java.lang.Byte.MAX_VALUE.toLong
        val minByte: java.lang.Long = java.lang.Byte.MIN_VALUE.toLong
        val maxShort: java.lang.Long = java.lang.Short.MAX_VALUE.toLong
        val minShort: java.lang.Long = java.lang.Short.MIN_VALUE.toLong
        val maxInteger: java.lang.Long = java.lang.Integer.MAX_VALUE.toLong
        val minInteger: java.lang.Long = java.lang.Integer.MIN_VALUE.toLong
        val maxLong: java.lang.Long = java.lang.Long.MAX_VALUE
        val minLong: java.lang.Long = java.lang.Long.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxLong, one)
        val row3 = Row(3, minByte, minShort, minShort, minInteger, minLong, zero)
        val row4 = Row(4, null, b, null, a, a, one)
        val row5 = Row(5, b, b, b, a, a, zero)
        val row6 = Row(6, b, b, null, a, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType),
            StructField("c3", LongType),
            StructField("c4", LongType),
            StructField("c5", LongType),
            StructField("c6", LongType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Float to SIGNED") {
    // success
    // java.lang.Float -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Float = java.lang.Float.valueOf("1")
        val zero: java.lang.Float = java.lang.Float.valueOf("0")
        val a: java.lang.Float = java.lang.Float.valueOf("11")
        val b: java.lang.Float = java.lang.Float.valueOf("-11")

        val maxByte: java.lang.Float = java.lang.Byte.MAX_VALUE.toFloat
        val minByte: java.lang.Float = java.lang.Byte.MIN_VALUE.toFloat
        val maxShort: java.lang.Float = java.lang.Short.MAX_VALUE.toFloat
        val minShort: java.lang.Float = java.lang.Short.MIN_VALUE.toFloat

        // `-100` & `+100` because of
        // com.mysql.jdbc.MysqlDataTruncation: Data truncation: Out of range value for column 'c4' at row 1
        val maxInteger: java.lang.Float = java.lang.Integer.MAX_VALUE.toFloat - 100
        val minInteger: java.lang.Float = java.lang.Integer.MIN_VALUE.toFloat + 100

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger, one)
        val row3 = Row(3, minByte, minShort, minShort, minInteger, minInteger, zero)
        val row4 = Row(4, null, b, null, a, a, one)
        val row5 = Row(5, b, b, b, a, a, zero)
        val row6 = Row(6, b, b, null, a, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType),
            StructField("c3", FloatType),
            StructField("c4", FloatType),
            StructField("c5", FloatType),
            StructField("c6", FloatType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Double to SIGNED") {
    // success
    // java.lang.Double -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Double = java.lang.Double.valueOf("1")
        val zero: java.lang.Double = java.lang.Double.valueOf("0")
        val a: java.lang.Double = java.lang.Double.valueOf("11")
        val b: java.lang.Double = java.lang.Double.valueOf("-11")

        val maxByte: java.lang.Double = java.lang.Byte.MAX_VALUE.toDouble
        val minByte: java.lang.Double = java.lang.Byte.MIN_VALUE.toDouble
        val maxShort: java.lang.Double = java.lang.Short.MAX_VALUE.toDouble
        val minShort: java.lang.Double = java.lang.Short.MIN_VALUE.toDouble
        val maxInteger: java.lang.Double = java.lang.Integer.MAX_VALUE.toDouble
        val minInteger: java.lang.Double = java.lang.Integer.MIN_VALUE.toDouble

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger, one)
        val row3 = Row(3, minByte, minShort, minShort, minInteger, minInteger, zero)
        val row4 = Row(4, null, b, null, a, a, one)
        val row5 = Row(5, b, b, b, a, a, zero)
        val row6 = Row(6, b, b, null, a, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DoubleType),
            StructField("c2", DoubleType),
            StructField("c3", DoubleType),
            StructField("c4", DoubleType),
            StructField("c5", DoubleType),
            StructField("c6", DoubleType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from String to SIGNED") {
    // success
    // String -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} SIGNED BOOLEAN
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.String = "11"
        val b: java.lang.String = "-11"

        val maxByte: java.lang.String = java.lang.Byte.MAX_VALUE.toString
        val minByte: java.lang.String = java.lang.Byte.MIN_VALUE.toString
        val maxShort: java.lang.String = java.lang.Short.MAX_VALUE.toString
        val minShort: java.lang.String = java.lang.Short.MIN_VALUE.toString
        val maxInteger: java.lang.String = java.lang.Integer.MAX_VALUE.toString
        val minInteger: java.lang.String = java.lang.Integer.MIN_VALUE.toString

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger, "1")
        val row3 = Row(3, minByte, minShort, minShort, minInteger, minInteger, "0")
        val row4 = Row(4, null, b, null, a, a, "1")
        val row5 = Row(5, b, b, b, a, a, "0")
        val row6 = Row(6, b, b, null, a, null, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType),
            StructField("c3", StringType),
            StructField("c4", StringType),
            StructField("c5", StringType),
            StructField("c6", StringType)))

        val aRead: java.lang.Long = 11L
        val bRead: java.lang.Long = -11L

        val maxByteRead: java.lang.Long = java.lang.Byte.MAX_VALUE.toLong
        val minByteRead: java.lang.Long = java.lang.Byte.MIN_VALUE.toLong
        val maxShortRead: java.lang.Long = java.lang.Short.MAX_VALUE.toLong
        val minShortRead: java.lang.Long = java.lang.Short.MIN_VALUE.toLong
        val maxIntegerRead: java.lang.Long = java.lang.Integer.MAX_VALUE.toLong
        val minIntegerRead: java.lang.Long = java.lang.Integer.MIN_VALUE.toLong

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 =
          Row(2, maxByteRead, maxShortRead, maxShortRead, maxIntegerRead, maxIntegerRead, 1L)
        val readRow3 =
          Row(3, minByteRead, minShortRead, minShortRead, minIntegerRead, minIntegerRead, 0L)
        val readRow4 = Row(4, null, bRead, null, aRead, aRead, 1L)
        val readRow5 = Row(5, bRead, bRead, bRead, aRead, aRead, 0L)
        val readRow6 = Row(6, bRead, bRead, null, aRead, null, null)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema)
    }
  }

  // TODO: test following types
  // java.math.BigDecimal
  // java.sql.Date
  // java.sql.Timestamp
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  private def createTable(): Unit =
    jdbcUpdate(
      s"create table $dbtable(i INT, c1 TINYINT, c2 SMALLINT, c3 MEDIUMINT, c4 INT, c5 BIGINT, c6 BOOLEAN)")
}
