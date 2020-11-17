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
 * DECIMAL type include:
 * 1. DECIMAL
 */
class ToDecimalSuite extends BaseBatchWriteTest("test_data_type_convert_to_decimal") {

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", DecimalType(38, 1)),
      StructField("c2", DecimalType(38, 2)),
      StructField("c3", DecimalType(38, 3)),
      StructField("c4", DecimalType(38, 4)),
      StructField("c5", DecimalType(38, 5)),
      StructField("c6", DecimalType(38, 6))))

  test("Test Convert from java.lang.Boolean to DECIMAL") {
    // success
    // java.lang.Boolean -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Boolean = true
        val b: java.lang.Boolean = false

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, a, a, a, a, a, a)
        val row5 = Row(5, b, b, b, b, b, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType),
            StructField("c3", BooleanType),
            StructField("c4", BooleanType),
            StructField("c5", BooleanType),
            StructField("c6", BooleanType)))

        val readA1: java.math.BigDecimal = java.math.BigDecimal.valueOf(10, 1)
        val readA2: java.math.BigDecimal = java.math.BigDecimal.valueOf(100, 2)
        val readA3: java.math.BigDecimal = java.math.BigDecimal.valueOf(1000, 3)
        val readA4: java.math.BigDecimal = java.math.BigDecimal.valueOf(10000, 4)
        val readA5: java.math.BigDecimal = java.math.BigDecimal.valueOf(100000, 5)
        val readA6: java.math.BigDecimal = java.math.BigDecimal.valueOf(1000000, 6)
        val readB1: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 1)
        val readB2: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 2)
        val readB3: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 3)
        val readB4: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 4)
        val readB5: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 5)
        val readB6: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 6)

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA1, readB2, readA3, readB4, readA5, readB6)
        val readRow3 = Row(3, readB1, readA2, readB3, readA4, readB5, readA6)
        val readRow4 = Row(4, readA1, readA2, readA3, readA4, readA5, readA6)
        val readRow5 = Row(5, readB1, readB2, readB3, readB4, readB5, readB6)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Byte to DECIMAL") {
    // success
    // java.lang.Byte -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.valueOf("22")
        val c: java.lang.Byte = java.lang.Byte.MAX_VALUE
        val d: java.lang.Byte = java.lang.Byte.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from java.lang.Short to DECIMAL") {
    // success
    // java.lang.Short -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("22")
        val c: java.lang.Short = java.lang.Short.MAX_VALUE
        val d: java.lang.Short = java.lang.Short.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from java.lang.Integer to DECIMAL") {
    // success
    // java.lang.Integer -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Integer = java.lang.Integer.valueOf("11")
        val b: java.lang.Integer = java.lang.Integer.valueOf("22")
        val c: java.lang.Integer = java.lang.Integer.MAX_VALUE - 1
        val d: java.lang.Integer = java.lang.Integer.MIN_VALUE + 1

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from java.lang.Long to DECIMAL") {
    // success
    // java.lang.Long -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Long = java.lang.Long.valueOf("11")
        val b: java.lang.Long = java.lang.Long.valueOf("22")
        val c: java.lang.Long = java.lang.Long.MAX_VALUE
        val d: java.lang.Long = java.lang.Long.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from java.lang.Float to DECIMAL") {
    // success
    // java.lang.Float -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a1: java.lang.Float = 1.1f
        val a2: java.lang.Float = -2.2f
        val a3: java.lang.Float = 1.2e-2f
        val a4: java.lang.Float = 1.4e-25f
        val a5: java.lang.Float = -1.4e-25f
        val a6: java.lang.Float = 1.88e-4f

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a1, a2, a3, a4, a5, a6)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType),
            StructField("c3", FloatType),
            StructField("c4", FloatType),
            StructField("c5", FloatType),
            StructField("c6", FloatType)))

        val readA1: java.math.BigDecimal = java.math.BigDecimal.valueOf(11, 1)
        val readA2: java.math.BigDecimal = java.math.BigDecimal.valueOf(-220, 2)
        val readA3: java.math.BigDecimal = java.math.BigDecimal.valueOf(12, 3)
        val readA4: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 4)
        val readA5: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 5)
        val readA6: java.math.BigDecimal = java.math.BigDecimal.valueOf(188, 6)

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA1, readA2, readA3, readA4, readA5, readA6)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(readRow1, readRow2), readSchema)
    }
  }

  test("Test Convert from java.lang.Double to DECIMAL") {
    // success
    // java.lang.Double -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a1: java.lang.Double = 1.1d
        val a2: java.lang.Double = -2.2d
        val a3: java.lang.Double = 1.2e-2d
        val a4: java.lang.Double = 1.4e-25d
        val a5: java.lang.Double = -1.4e-25d
        val a6: java.lang.Double = 1.88e-4d

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a1, a2, a3, a4, a5, a6)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DoubleType),
            StructField("c2", DoubleType),
            StructField("c3", DoubleType),
            StructField("c4", DoubleType),
            StructField("c5", DoubleType),
            StructField("c6", DoubleType)))

        val readA1: java.math.BigDecimal = java.math.BigDecimal.valueOf(11, 1)
        val readA2: java.math.BigDecimal = java.math.BigDecimal.valueOf(-220, 2)
        val readA3: java.math.BigDecimal = java.math.BigDecimal.valueOf(12, 3)
        val readA4: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 4)
        val readA5: java.math.BigDecimal = java.math.BigDecimal.valueOf(0, 5)
        val readA6: java.math.BigDecimal = java.math.BigDecimal.valueOf(188, 6)

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA1, readA2, readA3, readA4, readA5, readA6)
        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(readRow1, readRow2), readSchema)
    }
  }

  test("Test Convert from String to DECIMAL") {
    // success
    // java.lang.String -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.String = new java.lang.String("1.1")
        val b: java.lang.String = new java.lang.String("-2.2")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType),
            StructField("c3", StringType),
            StructField("c4", StringType),
            StructField("c5", StringType),
            StructField("c6", StringType)))

        val readA1: java.math.BigDecimal = java.math.BigDecimal.valueOf(11, 1)
        val readA3: java.math.BigDecimal = java.math.BigDecimal.valueOf(1100, 3)
        val readA5: java.math.BigDecimal = java.math.BigDecimal.valueOf(110000, 5)
        val readB2: java.math.BigDecimal = java.math.BigDecimal.valueOf(-220, 2)
        val readB4: java.math.BigDecimal = java.math.BigDecimal.valueOf(-22000, 4)
        val readB6: java.math.BigDecimal = java.math.BigDecimal.valueOf(-2200000, 6)

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA1, readB2, readA3, readB4, readA5, readB6)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(readRow1, readRow2), readSchema)
    }
  }

  test("Test Convert from java.math.BigDecimal to DECIMAL") {
    // failure
    // java.math.BigDecimal -> DECIMAL
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.math.BigDecimal = java.math.BigDecimal.ONE
        val b: java.math.BigDecimal = java.math.BigDecimal.ZERO
        val c: java.math.BigDecimal = java.math.BigDecimal.TEN
        val d: java.math.BigDecimal = java.math.BigDecimal.valueOf(1000000)

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DecimalType(38, 0)),
            StructField("c2", DecimalType(38, 0)),
            StructField("c3", DecimalType(38, 0)),
            StructField("c4", DecimalType(38, 0)),
            StructField("c5", DecimalType(38, 0)),
            StructField("c6", DecimalType(38, 0))))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  // TODO: test following types
  // java.sql.Date
  // java.sql.Timestamp
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  private def createTable(): Unit =
    jdbcUpdate(s"""create table $dbtable(i INT,
         |c1 DECIMAL(38, 1),
         |c2 DECIMAL(38, 2),
         |c3 DECIMAL(38, 3),
         |c4 DECIMAL(38, 4),
         |c5 DECIMAL(38, 5),
         |c6 DECIMAL(38, 6))""".stripMargin)
}
