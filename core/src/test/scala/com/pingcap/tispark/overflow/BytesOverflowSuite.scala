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

package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * BYTES type include:
 * 1. BINARY
 * 2. VARBINARY
 * 3. TINYBLOB
 * 4. BLOB
 * 5. MEDIUMBLOB
 * 6. LONGBLOB
 */
class BytesOverflowSuite extends BaseDataSourceTest("test_data_type_bytes_overflow") {

  test("Test BINARY Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBinaryOverflow(false)
  }

  test("Test BINARY as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testBinaryOverflow(true)
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }

  test("Test VARBINARY Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testVarbinaryOverflow(false)
  }

  test("Test VARBINARY as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testVarbinaryOverflow(true)
  }

  private def testBinaryOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BINARY(8), primary key (c1(4)))")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BINARY(8))")
    }

    val row = Row("0123456789")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "length 10 > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test TINYBLOB Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testTinyBlobOverflow(false)
  }

  test("Test TINYBLOB as key Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testTinyBlobOverflow(true)
  }

  private def testVarbinaryOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 VARBINARY(8), primary key (c1(4)))")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 VARBINARY(8))")
    }

    val row = Row("0123456789")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "length 10 > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  private def testTinyBlobOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 TINYBLOB, primary key (c1(4)))")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 TINYBLOB)")
    }

    val base = "0123456789"
    var str = ""
    for (_ <- 1 to 30) {
      str = str + base
    }
    val row = Row(str)

    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "length 300 > max length 255"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }
}
