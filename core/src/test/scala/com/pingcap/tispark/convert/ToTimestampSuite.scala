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
 * TIMESTAMP type include:
 * 1. TIMESTAMP
 */
class ToTimestampSuite extends BaseBatchWriteTest("test_data_type_convert_to_timestamp") {

  private val readSchema = StructType(
    List(
      StructField("i", IntegerType),
      StructField("c1", TimestampType),
      StructField("c2", TimestampType)))

  ignore("Test Convert from java.lang.Long to TIMESTAMP") {
    // success
    // java.lang.Long -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val a: java.lang.Long = java.sql.Timestamp.valueOf("2019-11-11 11:11:11").getTime
        val b: java.lang.Long = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999999").getTime

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", LongType),
            StructField("c2", LongType)))

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999999")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(readRow1, readRow2), readSchema)
      case (writeFunc, "jdbcWrite") =>
      // TODO: ignored, because of this error
      //java.sql.BatchUpdateException: Data truncation: invalid time format: '34'
    }
  }

  test("Test Convert from String to TIMESTAMP") {
    // success
    // String -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val row1 = Row(1, null, null)
        val row2 = Row(2, "2019-11-11 11:11:11", "1990-01-01 01:01:01.999999")

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", StringType),
            StructField("c2", StringType)))

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999999")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(readRow1, readRow2), readSchema)
      case (writeFunc, "jdbcWrite") =>
      // ignored because spark jdbc write does not use local time zone
    }
  }

  test("Test Convert from java.sql.Date to TIMESTAMP") {
    // success
    // java.sql.Date -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val a: java.sql.Date = java.sql.Date.valueOf("2019-11-11")
        val b: java.sql.Date = java.sql.Date.valueOf("1990-01-01")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", DateType),
            StructField("c2", DateType)))

        val readA: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 00:00:00.0")
        val readB: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 00:00:00.0")

        val readRow1 = Row(1, null, null)
        val readRow2 = Row(2, readA, readB)

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(readRow1, readRow2), readSchema)

      case (writeFunc, "jdbcWrite") =>
      // ignored because spark jdbc write does not use local time zone
    }
  }

  test("Test Convert from java.sql.Timestamp to TIMESTAMP") {
    // success
    // java.sql.Timestamp -> TIMESTAMP
    compareTiDBWriteWithJDBC {
      case (writeFunc, "tidbWrite") =>
        val a: java.sql.Timestamp = java.sql.Timestamp.valueOf("2019-11-11 11:11:11")
        val b: java.sql.Timestamp = java.sql.Timestamp.valueOf("1990-01-01 01:01:01.999999")

        val row1 = Row(1, null, null)
        val row2 = Row(2, a, b)

        val schema = StructType(
          List(
            StructField("i", IntegerType),
            StructField("c1", TimestampType),
            StructField("c2", TimestampType)))

        dropTable()
        createTable()

        // insert rows
        writeFunc(List(row1, row2), schema, None)
        compareTiDBSelectWithJDBC(List(row1, row2), readSchema)

      case (writeFunc, "jdbcWrite") =>
      // ignored because spark jdbc write does not use local time zone
    }
  }

  // TODO: test following types
  // java.math.BigDecimal
  // java.lang.Boolean
  // java.lang.Byte
  // java.lang.Short
  // java.lang.Integer
  // java.lang.Float
  // java.lang.Double
  // Array[String]
  // scala.collection.Seq
  // scala.collection.Map
  // org.apache.spark.sql.Row

  private def createTable(): Unit =
    jdbcUpdate(s"create table $dbtable(i INT, c1 TIMESTAMP, c2 TIMESTAMP(6))")
}
