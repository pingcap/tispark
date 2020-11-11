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
 * UNSIGNED type include:
 * 1. TINYINT UNSIGNED
 * 2. SMALLINT UNSIGNED
 * 3. MEDIUMINT UNSIGNED
 * 4. INT UNSIGNED
 * 5. BIGINT UNSIGNED
 */
class ToUnsignedSuite extends BaseBatchWriteTest("test_data_type_convert_to_unsigned") {
  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", LongType),
      StructField("c2", LongType),
      StructField("c3", LongType),
      StructField("c4", LongType),
      StructField("c5", LongType)))

  test("Test Convert from java.lang.Boolean to UNSIGNED") {
    // success
    // java.lang.Boolean -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, null, true, true, true, true)
        val row3 = Row(3, false, null, false, false, false)
        val row4 = Row(4, true, false, null, false, true)
        val row5 = Row(5, true, false, false, null, true)
        val row6 = Row(6, true, false, true, false, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType),
            StructField("c3", BooleanType),
            StructField("c4", BooleanType),
            StructField("c5", BooleanType)))

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 = Row(2, null, 1L, 1L, 1L, 1L)
        val readRow3 = Row(3, 0L, null, 0L, 0L, 0L)
        val readRow4 = Row(4, 1L, 0L, null, 0L, 1L)
        val readRow5 = Row(5, 1L, 0L, 0L, null, 1L)
        val readRow6 = Row(6, 1L, 0L, 1L, 0L, null)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Byte to UNSIGNED") {
    // success
    // java.lang.Byte -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.MAX_VALUE
        val c: java.lang.Byte = java.lang.Byte.valueOf("22")
        val d: java.lang.Byte = java.lang.Byte.valueOf("0")

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, null, a, b, c, d)
        val row3 = Row(3, b, null, d, a, a)
        val row4 = Row(4, c, c, null, a, d)
        val row5 = Row(5, b, b, b, null, a)
        val row6 = Row(6, c, c, a, d, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ByteType),
            StructField("c2", ByteType),
            StructField("c3", ByteType),
            StructField("c4", ByteType),
            StructField("c5", ByteType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Short to UNSIGNED") {
    // success
    // java.lang.Short -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("22")
        val zero: java.lang.Short = java.lang.Short.valueOf("0")

        val maxByte: java.lang.Short = java.lang.Byte.MAX_VALUE.toShort
        val maxShort: java.lang.Short = java.lang.Short.MAX_VALUE

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxShort, maxShort)
        val row3 = Row(3, zero, zero, zero, zero, zero)
        val row4 = Row(4, null, b, null, a, a)
        val row5 = Row(5, b, b, b, a, a)
        val row6 = Row(6, b, b, null, a, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ShortType),
            StructField("c2", ShortType),
            StructField("c3", ShortType),
            StructField("c4", ShortType),
            StructField("c5", ShortType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Integer to UNSIGNED") {
    // success
    // java.lang.Integer -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Integer = java.lang.Integer.valueOf("11")
        val b: java.lang.Integer = java.lang.Integer.valueOf("22")
        val zero: java.lang.Integer = java.lang.Integer.valueOf("0")

        val maxByte: java.lang.Integer = java.lang.Byte.MAX_VALUE.toInt
        val maxShort: java.lang.Integer = java.lang.Short.MAX_VALUE.toInt
        val maxInteger: java.lang.Integer = java.lang.Integer.MAX_VALUE

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger)
        val row3 = Row(3, zero, zero, zero, zero, zero)
        val row4 = Row(4, null, b, null, a, a)
        val row5 = Row(5, b, b, b, a, a)
        val row6 = Row(6, b, b, null, a, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", IntegerType),
            StructField("c2", IntegerType),
            StructField("c3", IntegerType),
            StructField("c4", IntegerType),
            StructField("c5", IntegerType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Long to UNSIGNED") {
    // success
    // java.lang.Long -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Long = java.lang.Long.valueOf("11")
        val b: java.lang.Long = java.lang.Long.valueOf("22")
        val zero: java.lang.Long = java.lang.Long.valueOf("0")

        val maxByte: java.lang.Long = java.lang.Byte.MAX_VALUE.toLong
        val maxShort: java.lang.Long = java.lang.Short.MAX_VALUE.toLong
        val maxInteger: java.lang.Long = java.lang.Integer.MAX_VALUE.toLong
        val maxLong: java.lang.Long = java.lang.Long.MAX_VALUE

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxLong)
        val row3 = Row(3, zero, zero, zero, zero, zero)
        val row4 = Row(4, null, b, null, a, a)
        val row5 = Row(5, b, b, b, a, a)
        val row6 = Row(6, b, b, null, a, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType),
            StructField("c3", LongType),
            StructField("c4", LongType),
            StructField("c5", LongType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5, row6), schema)
    }
  }

  test("Test Convert from java.lang.Float to UNSIGNED") {
    // success
    // java.lang.Float -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Float = java.lang.Float.valueOf("11.1")
        val b: java.lang.Float = java.lang.Float.valueOf("22.2")
        val zero: java.lang.Float = java.lang.Float.valueOf("0")

        val maxByte: java.lang.Float = java.lang.Byte.MAX_VALUE.toFloat
        val maxShort: java.lang.Float = java.lang.Short.MAX_VALUE.toFloat

        val medianInteger: java.lang.Float = 10737418f

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, medianInteger, medianInteger)
        val row3 = Row(3, zero, zero, zero, zero, zero)
        val row4 = Row(4, null, b, null, a, a)
        val row5 = Row(5, b, b, b, a, a)
        val row6 = Row(6, b, b, null, a, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType),
            StructField("c3", FloatType),
            StructField("c4", FloatType),
            StructField("c5", FloatType)))

        val readA: java.lang.Long = 11L
        val readB: java.lang.Long = 22L
        val readZero: java.lang.Long = 0L
        val readMaxByte: java.lang.Long = java.lang.Byte.MAX_VALUE.toLong
        val readMaxShort: java.lang.Long = java.lang.Short.MAX_VALUE.toLong
        val readMedianInteger: java.lang.Long = 10737418L

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 =
          Row(2, readMaxByte, readMaxShort, readMaxShort, readMedianInteger, readMedianInteger)
        val readRow3 = Row(3, readZero, readZero, readZero, readZero, readZero)
        val readRow4 = Row(4, null, readB, null, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readA, readA)
        val readRow6 = Row(6, readB, readB, null, readA, null)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Double to UNSIGNED") {
    // success
    // java.lang.Double -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Double = java.lang.Double.valueOf("11.1")
        val b: java.lang.Double = java.lang.Double.valueOf("22.2")
        val zero: java.lang.Double = java.lang.Double.valueOf("0")

        val maxByte: java.lang.Double = java.lang.Byte.MAX_VALUE.toDouble
        val maxShort: java.lang.Double = java.lang.Short.MAX_VALUE.toDouble
        val maxInteger: java.lang.Double = java.lang.Integer.MAX_VALUE.toDouble

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger)
        val row3 = Row(3, zero, zero, zero, zero, zero)
        val row4 = Row(4, null, b, null, a, a)
        val row5 = Row(5, b, b, b, a, a)
        val row6 = Row(6, b, b, null, a, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DoubleType),
            StructField("c2", DoubleType),
            StructField("c3", DoubleType),
            StructField("c4", DoubleType),
            StructField("c5", DoubleType)))

        val readA: java.lang.Long = 11L
        val readB: java.lang.Long = 22L
        val readZero: java.lang.Long = 0L
        val readMaxByte: java.lang.Long = java.lang.Byte.MAX_VALUE.toLong
        val readMaxShort: java.lang.Long = java.lang.Short.MAX_VALUE.toLong
        val readMaxInteger: java.lang.Long = java.lang.Integer.MAX_VALUE.toLong

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 =
          Row(2, readMaxByte, readMaxShort, readMaxShort, readMaxInteger, readMaxInteger)
        val readRow3 = Row(3, readZero, readZero, readZero, readZero, readZero)
        val readRow4 = Row(4, null, readB, null, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readA, readA)
        val readRow6 = Row(6, readB, readB, null, readA, null)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema)
    }
  }

  test("Test Convert from String to UNSIGNED") {
    // success
    // String -> {TINYINT SMALLINT MEDIUMINT INT BIGINT} UNSIGNED
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: String = "11"
        val b: String = "22"
        val zero: String = "0"

        val maxByte: String = java.lang.Byte.MAX_VALUE.toString
        val maxShort: String = java.lang.Short.MAX_VALUE.toString
        val maxInteger: String = java.lang.Integer.MAX_VALUE.toString

        val row1 = Row(1, null, null, null, null, null)
        val row2 = Row(2, maxByte, maxShort, maxShort, maxInteger, maxInteger)
        val row3 = Row(3, zero, zero, zero, zero, zero)
        val row4 = Row(4, null, b, null, a, a)
        val row5 = Row(5, b, b, b, a, a)
        val row6 = Row(6, b, b, null, a, null)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType),
            StructField("c3", StringType),
            StructField("c4", StringType),
            StructField("c5", StringType)))

        val readA: java.lang.Long = 11L
        val readB: java.lang.Long = 22L
        val readZero: java.lang.Long = 0L
        val readMaxByte: java.lang.Long = java.lang.Byte.MAX_VALUE.toLong
        val readMaxShort: java.lang.Long = java.lang.Short.MAX_VALUE.toLong
        val readMaxInteger: java.lang.Long = java.lang.Integer.MAX_VALUE.toLong

        val readRow1 = Row(1, null, null, null, null, null)
        val readRow2 =
          Row(2, readMaxByte, readMaxShort, readMaxShort, readMaxInteger, readMaxInteger)
        val readRow3 = Row(3, readZero, readZero, readZero, readZero, readZero)
        val readRow4 = Row(4, null, readB, null, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readA, readA)
        val readRow6 = Row(6, readB, readB, null, readA, null)

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
    jdbcUpdate(s"""create table $dbtable(i INT,
         | c1 TINYINT UNSIGNED,
         | c2 SMALLINT UNSIGNED,
         | c3 MEDIUMINT UNSIGNED,
         | c4 INT UNSIGNED,
         | c5 BIGINT UNSIGNED)""".stripMargin)
}
