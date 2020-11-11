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
 * SRTING type include:
 * 1. CHAR
 * 2. VARCHAR
 * 3. TINYTEXT
 * 4. TEXT
 * 5. MEDIUMTEXT
 * 6. LONGTEXT
 */
class ToStringSuite extends BaseBatchWriteTest("test_data_type_convert_to_string") {

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", StringType),
      StructField("c2", StringType),
      StructField("c3", StringType),
      StructField("c4", StringType),
      StructField("c5", StringType),
      StructField("c6", StringType)))

  test("Test Convert from java.lang.Boolean to STRING") {
    // success
    // java.lang.Boolean -> STRING
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

        val readA: java.lang.String = "1"
        val readB: java.lang.String = "0"

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readA, readA, readA, readA, readA, readA)
        val readRow5 = Row(5, readB, readB, readB, readB, readB, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Byte to STRING") {
    // success
    // java.lang.Byte -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.valueOf("-22")
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

        val readA: java.lang.String = "11"
        val readB: java.lang.String = "-22"
        val readC: java.lang.String = java.lang.Byte.MAX_VALUE.toString
        val readD: java.lang.String = java.lang.Byte.MIN_VALUE.toString

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Short to STRING") {
    // success
    // java.lang.Short -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("-22")
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

        val readA: java.lang.String = "11"
        val readB: java.lang.String = "-22"
        val readC: java.lang.String = java.lang.Short.MAX_VALUE.toString
        val readD: java.lang.String = java.lang.Short.MIN_VALUE.toString

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Integer to STRING") {
    // success
    // java.lang.Integer -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Integer = java.lang.Integer.valueOf("11")
        val b: java.lang.Integer = java.lang.Integer.valueOf("-22")
        val c: java.lang.Integer = java.lang.Integer.MAX_VALUE
        val d: java.lang.Integer = java.lang.Integer.MIN_VALUE

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

        val readA: java.lang.String = "11"
        val readB: java.lang.String = "-22"
        val readC: java.lang.String = java.lang.Integer.MAX_VALUE.toString
        val readD: java.lang.String = java.lang.Integer.MIN_VALUE.toString

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.lang.Long to STRING") {
    // success
    // java.lang.Long -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Long = java.lang.Long.valueOf("11")
        val b: java.lang.Long = java.lang.Long.valueOf("-22")
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

        val readA: java.lang.String = "11"
        val readB: java.lang.String = "-22"
        val readC: java.lang.String = java.lang.Long.MAX_VALUE.toString
        val readD: java.lang.String = java.lang.Long.MIN_VALUE.toString

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  // TODO: float -> string problem
  // 3.4028235E38 -> 340282350000000000000000000000000000000
  ignore("Test Convert from java.lang.Float to STRING") {
    // success
    // java.lang.Float -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Float = java.lang.Float.valueOf("1.1")
        val b: java.lang.Float = java.lang.Float.valueOf("-2.2")
        val c: java.lang.Float = java.lang.Float.MAX_VALUE
        val d: java.lang.Float = java.lang.Float.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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

        val readA: java.lang.String = "1.1"
        val readB: java.lang.String = "-2.2"
        val readC: java.lang.String = java.lang.Float.MAX_VALUE.toString
        val readD: java.lang.String = java.lang.Float.MIN_VALUE.toString

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        // TODO: TiSpark transforms FLOAT to Double, which will cause precision problem,
        //  so we just set skipTiDBAndExpectedAnswerCheck = true
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true)
    }
  }

  // TODO: java.sql.BatchUpdateException: Data truncation: Data too long for column 'c1' at row 1
  ignore("Test Convert from java.lang.Double to STRING") {
    // success
    // java.lang.Double -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Double = java.lang.Double.valueOf("1.1")
        val b: java.lang.Double = java.lang.Double.valueOf("-2.2")
        val c: java.lang.Double = java.lang.Double.MAX_VALUE
        val d: java.lang.Double = java.lang.Double.MIN_VALUE

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from String to STRING") {
    // success
    // java.lang.String -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.String = new java.lang.String("1.1")
        val b: java.lang.String = new java.lang.String("-2.2")
        val c: java.lang.String = new java.lang.String("abcdafdasdf")
        val d: java.lang.String = new java.lang.String("safefasgsdfg")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
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
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4, row5), schema)
    }
  }

  test("Test Convert from java.math.BigDecimal to STRING") {
    // failure
    // java.math.BigDecimal -> STRING
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

        val readA: java.lang.String = a.toString
        val readB: java.lang.String = b.toString
        val readC: java.lang.String = c.toString
        val readD: java.lang.String = d.toString

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.sql.Date to STRING") {
    // failure
    // java.sql.Date -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Date = java.sql.Date.valueOf("2019-01-01")
        val b: java.sql.Date = java.sql.Date.valueOf("1990-12-12")
        val c: java.sql.Date = java.sql.Date.valueOf("2000-09-09")
        val d: java.sql.Date = java.sql.Date.valueOf("1999-10-28")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DateType),
            StructField("c2", DateType),
            StructField("c3", DateType),
            StructField("c4", DateType),
            StructField("c5", DateType),
            StructField("c6", DateType)))

        val readA: java.lang.String = "2019-01-01"
        val readB: java.lang.String = "1990-12-12"
        val readC: java.lang.String = "2000-09-09"
        val readD: java.lang.String = "1999-10-28"

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5),
          readSchema)
    }
  }

  test("Test Convert from java.sql.Timestamp to STRING") {
    // success
    // java.sql.Timestamp -> STRING
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-01-01 01:01:01")
        val b: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-12-12 11:11:11.111")
        val c: java.sql.Timestamp = java.sql.Timestamp.valueOf("2000-09-09 03:03:03.123")
        val d: java.sql.Timestamp = java.sql.Timestamp.valueOf("1999-10-28 23:23:23.232")

        val row1 = Row(1, null, null, null, null, null, null)
        val row2 = Row(2, a, b, a, b, a, b)
        val row3 = Row(3, b, a, b, a, b, a)
        val row4 = Row(4, c, d, c, d, c, d)
        val row5 = Row(5, d, c, d, c, d, c)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", TimestampType),
            StructField("c2", TimestampType),
            StructField("c3", TimestampType),
            StructField("c4", TimestampType),
            StructField("c5", TimestampType),
            StructField("c6", TimestampType)))

        val readA: java.lang.String = "2019-01-01 01:01:01"
        val readB: java.lang.String = "1990-12-12 11:11:11.111"
        val readC: java.lang.String = "2000-09-09 03:03:03.123"
        val readD: java.lang.String = "1999-10-28 23:23:23.232"

        val readRow1 = Row(1, null, null, null, null, null, null)
        val readRow2 = Row(2, readA, readB, readA, readB, readA, readB)
        val readRow3 = Row(3, readB, readA, readB, readA, readB, readA)
        val readRow4 = Row(4, readC, readD, readC, readD, readC, readD)
        val readRow5 = Row(5, readD, readC, readD, readC, readD, readC)

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
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  private def createTable(): Unit =
    jdbcUpdate(
      s"create table $dbtable(i INT, c1 CHAR(255), c2 VARCHAR(255), c3 TINYTEXT, c4 TEXT, c5 MEDIUMTEXT, c6 LONGTEXT)")
}
