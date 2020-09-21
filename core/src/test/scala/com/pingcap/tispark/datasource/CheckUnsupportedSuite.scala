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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class CheckUnsupportedSuite extends BaseBatchWriteTest("test_datasource_check_unsupported") {

  override def beforeAll(): Unit =
    super.beforeAll()

  test("Test write to partition table") {
    tidbStmt.execute("set @@tidb_enable_table_partition = 1")

    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(128)) partition by range(i) (partition p0 values less than maxvalue)")
    jdbcUpdate(s"insert into $dbtable values(null, 'Hello')")

    val row1 = Row(null, "Hello")
    val row2 = Row(2, "TiDB")
    val row3 = Row(3, "Spark")

    val schema = StructType(List(StructField("i", IntegerType), StructField("s", StringType)))

    {
      val caught = intercept[TiBatchWriteException] {
        tidbWrite(List(row2, row3), schema)
      }
      assert(
        caught.getMessage
          .equals("tispark currently does not support write data to partition table!"))
    }

    testTiDBSelect(Seq(row1))
  }

  test("Check Virtual Generated Column") {
    jdbcUpdate(s"create table $dbtable(i INT, c1 INT, c2 INT,  c3 INT AS (c1 + c2))")

    val row1 = Row(1, 2, 3)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", IntegerType),
        StructField("c2", IntegerType)))

    val caught = intercept[TiBatchWriteException] {
      tidbWrite(List(row1), schema)
    }
    assert(
      caught.getMessage
        .equals("tispark currently does not support write data to table with generated column!"))

  }

  test("Check Stored Generated Column") {
    jdbcUpdate(s"create table $dbtable(i INT, c1 INT, c2 INT,  c3 INT AS (c1 + c2) STORED)")

    val row1 = Row(1, 2, 3)
    val schema = StructType(
      List(
        StructField("i", IntegerType),
        StructField("c1", IntegerType),
        StructField("c2", IntegerType)))
    val caught = intercept[TiBatchWriteException] {
      tidbWrite(List(row1), schema)
    }
    assert(
      caught.getMessage
        .equals("tispark currently does not support write data to table with generated column!"))

  }
}
