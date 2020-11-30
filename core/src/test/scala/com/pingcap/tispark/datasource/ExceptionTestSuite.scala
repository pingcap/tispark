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

package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class ExceptionTestSuite extends BaseBatchWriteTest("test_datasource_exception_test") {

  test("Test write to table does not exist") {
    val row1 = Row(null, "Hello")
    val row2 = Row(2L, "TiDB")

    val schema = StructType(List(StructField("i", LongType), StructField("s", StringType)))

    val caught = intercept[TiBatchWriteException] {
      tidbWrite(List(row1, row2), schema)
    }
    assert(caught.getMessage.equals(s"table $dbtable does not exists!"))
  }

  test("Test column does not exist") {
    val row1 = Row(2L, 3L)

    val schema = StructType(List(StructField("i", LongType), StructField("i2", LongType)))

    jdbcUpdate(s"create table $dbtable(i int)")

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row1), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "table without auto increment column, but data col size 2 != table column size 1"))
    }
  }

  test("Missing insert column") {
    val row1 = Row(2L, 3L)

    val schema = StructType(List(StructField("i", LongType), StructField("i2", LongType)))

    jdbcUpdate(s"create table $dbtable(i int, i2 int, i3 int)")

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row1), schema)
      }
      assert(
        caught.getMessage
          .equals(
            "table without auto increment column, but data col size 2 != table column size 3"))
    }
  }

  test("Insert null value to Not Null Column") {
    val row1 = Row(null, 3L)
    val row2 = Row(4L, null)

    val schema = StructType(List(StructField("i", LongType), StructField("i2", LongType)))

    jdbcUpdate(s"create table $dbtable(i int, i2 int NOT NULL)")

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row1, row2), schema)
      }
      assert(
        caught.getMessage
          .equals("Insert null value to not null column! rows contain illegal null values!"))
    }
  }

  test("Insert to table with expression index") {
    if (!supportExpressionIndex) {
      cancel("current version of TiDB does not support expression index!")
    }

    val row1 = Row("a")
    val row2 = Row("PingCAP")

    val schema = StructType(List(StructField("name", StringType)))

    jdbcUpdate(s"create table $dbtable(name varchar(64))")
    jdbcUpdate(s"CREATE INDEX idx ON $dbtable ((lower(name)));")

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row1, row2), schema)
      }
      assert(
        caught.getMessage.equals(
          "tispark currently does not support write data to table with generated column!"))
    }
  }
}
