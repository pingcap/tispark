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
 * REAL type include:
 * 1. FLOAT
 * 2. DOUBLE
 */
class ToRealSuite extends BaseBatchWriteTest("test_data_type_convert_to_real") {

  //  + 1.0E-40f because of this issue: https://github.com/pingcap/tidb/issues/10587
  private val minFloat: java.lang.Float = java.lang.Float.MIN_VALUE + 1.0e-40f
  private val maxFloat: java.lang.Float = java.lang.Float.MAX_VALUE
  private val minDouble: java.lang.Double = java.lang.Double.MIN_VALUE
  private val maxDouble: java.lang.Double = java.lang.Double.MAX_VALUE

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", FloatType),
      StructField("c2", DoubleType)))

  test("Test Convert from java.lang.Boolean to REAL") {

    // success
    // java.lang.Boolean -> FLOAT
    // java.lang.Boolean -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, true, false)
        val row3 = Row(3, false, true)
        val row4 = Row(4, true, true)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", BooleanType),
            StructField("c2", BooleanType)))

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, 1f, 0d)
        val readRow3 = Row(3, 0f, 1d)
        val readRow4 = Row(4, 1f, 1d)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4),
          readSchema,
          skipJDBCReadCheck = true)
    }
  }

  test("Test Convert from java.lang.Byte to REAL") {
    // success
    // java.lang.Byte -> FLOAT
    // java.lang.Byte -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Byte = java.lang.Byte.valueOf("11")
        val b: java.lang.Byte = java.lang.Byte.valueOf("22")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)
        val row3 = Row(3, java.lang.Byte.MAX_VALUE, java.lang.Byte.MIN_VALUE)
        val row4 = Row(4, java.lang.Byte.MIN_VALUE, java.lang.Byte.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ByteType),
            StructField("c2", ByteType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4), schema, skipJDBCReadCheck = true)
    }
  }

  test("Test Convert from java.lang.Short to REAL") {
    // success
    // java.lang.Short -> FLOAT
    // java.lang.Short -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val a: java.lang.Short = java.lang.Short.valueOf("11")
        val b: java.lang.Short = java.lang.Short.valueOf("22")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)
        val row3 = Row(3, java.lang.Short.MAX_VALUE, java.lang.Short.MIN_VALUE)
        val row4 = Row(4, java.lang.Short.MIN_VALUE, java.lang.Short.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", ShortType),
            StructField("c2", ShortType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4), schema, skipJDBCReadCheck = true)
    }
  }

  test("Test Convert from java.lang.Integer to REAL") {
    // success
    // java.lang.Integer -> FLOAT
    // java.lang.Integer -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22, 33)
        val row3 = Row(3, java.lang.Integer.MAX_VALUE, java.lang.Integer.MIN_VALUE)
        val row4 = Row(4, java.lang.Integer.MIN_VALUE, java.lang.Integer.MAX_VALUE)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", IntegerType),
            StructField("c2", IntegerType)))

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, 22, 33d)
        val readRow3 =
          Row(3, java.lang.Integer.MAX_VALUE.toFloat, java.lang.Integer.MIN_VALUE.toDouble)
        val readRow4 = Row(4, java.lang.Integer.MIN_VALUE.toFloat, java.lang.Integer.MAX_VALUE)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4),
          readSchema,
          skipJDBCReadCheck = true)
    }
  }

  test("Test Convert from java.lang.Long to REAL") {
    // success
    // java.lang.Long -> FLOAT
    // java.lang.Long -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        val factor = 0.00000000001
        val max = (factor * java.lang.Long.MAX_VALUE).toLong
        val min = (factor * java.lang.Long.MIN_VALUE).toLong

        val row1 = Row(1, null, null)
        val row2 = Row(2, 22L, 33L)
        val row3 = Row(3, max, min)
        val row4 = Row(4, min, max)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2, row3, row4), schema, skipJDBCReadCheck = true)
    }
  }

  test("Test Convert from java.lang.Float to REAL") {
    // success
    // java.lang.Float -> FLOAT
    // java.lang.Float -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22.2f, 33.3f)
        val row3 = Row(3, maxFloat, maxFloat)
        val row4 = Row(4, minFloat, minFloat)
        val row5 = Row(5, -maxFloat, -maxFloat)
        val row6 = Row(6, -minFloat, -minFloat)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", FloatType),
            StructField("c2", FloatType)))

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, 22.2f, 33.3d)
        val readRow3 = Row(3, maxFloat, maxFloat.toDouble)
        val readRow4 = Row(4, minFloat, minFloat.toDouble)
        val readRow5 = Row(5, -maxFloat, (-maxFloat).toDouble)
        val readRow6 = Row(6, -minFloat, (-minFloat).toDouble)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)

        // TODO: TiSpark transforms FLOAT to Double, which will cause precision problem,
        //  so we just set skipTiDBAndExpectedAnswerCheck = true
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema,
          skipTiDBAndExpectedAnswerCheck = true)
      case (writeFunc, "jdbcWrite") =>
      //ignored, because of this error
      //com.mysql.jdbc.MysqlDataTruncation: Data truncation: Out of range value for column 'c1' at row 1
    }
  }

  test("Test Convert from java.lang.Double to REAL") {
    // success
    // java.lang.Double -> FLOAT
    // java.lang.Double -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        //  + 1.0E-40f because of this issue: https://github.com/pingcap/tidb/issues/10587
        val row1 = Row(1, null, null)
        val row2 = Row(2, 22.22d, 33.33d)
        val row3 = Row(3, maxFloat.toDouble, maxDouble)
        val row4 = Row(4, minFloat.toDouble, minDouble)
        val row5 = Row(5, -maxFloat.toDouble, -maxDouble)
        val row6 = Row(6, -minFloat.toDouble, -minDouble)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DoubleType),
            StructField("c2", DoubleType)))

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, 22.22f, 33.33d)
        val readRow3 = Row(3, maxFloat, maxDouble)
        val readRow4 = Row(4, minFloat, minDouble)
        val readRow5 = Row(5, -maxFloat, -maxDouble)
        val readRow6 = Row(6, -minFloat, -minDouble)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema,
          skipJDBCReadCheck = true)
    }
  }

  test("Test Convert from String to REAL") {
    // success
    // String -> FLOAT
    // String -> DOUBLE
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, "2.2", "-3.3")
        val row3 = Row(3, minFloat.toString, minDouble.toString)
        val row4 = Row(4, maxFloat.toString, maxDouble.toString)
        val row5 = Row(5, (-minFloat).toString, (-minDouble).toString)
        val row6 = Row(6, (-maxFloat + 1.0e20).toString, (-maxDouble).toString)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType)))

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, 2.2f, -3.3d)
        val readRow3 = Row(3, minFloat, minDouble)
        val readRow4 = Row(4, maxFloat, maxDouble)
        val readRow5 = Row(5, -minFloat, -minDouble)
        val readRow6 = Row(6, -maxFloat, -maxDouble)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2, row3, row4, row5, row6), schema, None)
        compareTiDBSelectWithJDBC(
          List(readRow1, readRow2, readRow3, readRow4, readRow5, readRow6),
          readSchema,
          skipJDBCReadCheck = true)
      case (writeFunc, "jdbcWrite") =>
      //ignored, because of this error
      //com.mysql.jdbc.MysqlDataTruncation: Data truncation: Out of range value for column 'c1' at row 1
    }
  }

  // TODO: test following types
  // java.sql.Date
  // java.math.BigDecimal
  // java.sql.Timestamp
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  private def createTable(): Unit =
    jdbcUpdate(s"create table $dbtable(i INT, c1 FLOAT, c2 DOUBLE)")
}
