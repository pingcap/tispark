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
import org.apache.spark.sql.types.{StructField, _}

/**
 * ENUM type include:
 * 1. ENUM
 */
class ToEnumSuite extends BaseBatchWriteTest("test_data_type_convert_to_enum") {

  private val readOne: java.lang.String = java.lang.String.valueOf("male")
  private val readTwo: java.lang.String = java.lang.String.valueOf("female")
  private val readA: java.lang.String = java.lang.String.valueOf("both")
  private val readB: java.lang.String = java.lang.String.valueOf("unknown")
  private val readC: java.lang.String = java.lang.String.valueOf("both")
  private val readD: java.lang.String = java.lang.String.valueOf("unknown")

  private val readRow1 = Row(1, null, null, null, null, null, null)
  private val readRow2 = Row(2, readOne, readB, readA, readB, readA, readB)
  private val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
  private val readRow4 = Row(4, readTwo, readD, readC, readD, readC, readD)
  private val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", StringType),
      StructField("c2", StringType),
      StructField("c3", StringType),
      StructField("c4", StringType),
      StructField("c5", StringType),
      StructField("c6", StringType)))

  test("Test Convert from java.lang.Boolean to ENUM") {
    // success
    // java.lang.Boolean -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, true, true, true, true, true, true)
        val row3 = Row(3, true, true, true, true, true, true)
        val row4 = Row(4, true, true, true, true, true, true)
        val row5 = Row(5, true, true, true, true, true, true)

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
        val readRow2 = Row(2, "male", "male", "male", "male", "male", "male")
        val readRow3 = Row(3, "male", "male", "male", "male", "male", "male")
        val readRow4 = Row(4, "male", "male", "male", "male", "male", "male")
        val readRow5 = Row(5, "male", "male", "male", "male", "male", "male")

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Byte to ENUM") {
    // success
    // java.lang.Byte -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Byte = java.lang.Byte.valueOf("1")
        val two: java.lang.Byte = java.lang.Byte.valueOf("2")
        val a: java.lang.Byte = java.lang.Byte.valueOf("3")
        val b: java.lang.Byte = java.lang.Byte.valueOf("4")
        val c: java.lang.Byte = java.lang.Byte.valueOf("3")
        val d: java.lang.Byte = java.lang.Byte.valueOf("4")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

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
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Short to ENUM") {
    // success
    // java.lang.Short -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Short = java.lang.Short.valueOf("1")
        val two: java.lang.Short = java.lang.Short.valueOf("2")
        val a: java.lang.Short = java.lang.Short.valueOf("3")
        val b: java.lang.Short = java.lang.Short.valueOf("4")
        val c: java.lang.Short = java.lang.Short.valueOf("3")
        val d: java.lang.Short = java.lang.Short.valueOf("4")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

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
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Integer to ENUM") {
    // success
    // java.lang.Integer -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Integer = java.lang.Integer.valueOf("1")
        val two: java.lang.Integer = java.lang.Integer.valueOf("2")
        val a: java.lang.Integer = java.lang.Integer.valueOf("3")
        val b: java.lang.Integer = java.lang.Integer.valueOf("4")
        val c: java.lang.Integer = java.lang.Integer.valueOf("3")
        val d: java.lang.Integer = java.lang.Integer.valueOf("4")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

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
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Long to ENUM") {
    // success
    // java.lang.Long -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Long = java.lang.Long.valueOf("1")
        val two: java.lang.Long = java.lang.Long.valueOf("2")
        val a: java.lang.Long = java.lang.Long.valueOf("3")
        val b: java.lang.Long = java.lang.Long.valueOf("4")
        val c: java.lang.Long = java.lang.Long.valueOf("3")
        val d: java.lang.Long = java.lang.Long.valueOf("4")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

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
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Float to ENUM") {
    // success
    // java.lang.Float -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Float = new java.lang.Float("1")
        val two: java.lang.Float = new java.lang.Float("2")
        val a: java.lang.Float = java.lang.Float.valueOf("3")
        val b: java.lang.Float = java.lang.Float.valueOf("4")
        val c: java.lang.Float = java.lang.Float.valueOf("3")
        val d: java.lang.Float = java.lang.Float.valueOf("4")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

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
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Double to ENUM") {
    // success
    // java.lang.Double -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.Double = new java.lang.Double("1")
        val two: java.lang.Double = new java.lang.Double("2")
        val a: java.lang.Double = java.lang.Double.valueOf("3")
        val b: java.lang.Double = java.lang.Double.valueOf("4")
        val c: java.lang.Double = java.lang.Double.valueOf("3")
        val d: java.lang.Double = java.lang.Double.valueOf("4")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

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
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from String to ENUM") {
    // success
    // java.lang.String -> ENUM
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val one: java.lang.String = new java.lang.String("1")
        val two: java.lang.String = new java.lang.String("2")
        val a: java.lang.String = new java.lang.String("both")
        val b: java.lang.String = new java.lang.String("unknown")
        val c: java.lang.String = new java.lang.String("both")
        val d: java.lang.String = new java.lang.String("unknown")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, one, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, two, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType),
            StructField("c3", StringType),
            StructField("c4", StringType),
            StructField("c5", StringType),
            StructField("c6", StringType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
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
                  |c1 ENUM('male', 'female', 'both', 'unknown'),
                  |c2 ENUM('male', 'female', 'both', 'unknown'),
                  |c3 ENUM('male', 'female', 'both', 'unknown'),
                  |c4 ENUM('male', 'female', 'both', 'unknown'),
                  |c5 ENUM('male', 'female', 'both', 'unknown'),
                  |c6 ENUM('male', 'female', 'both', 'unknown')
                  |)""".stripMargin)
}
