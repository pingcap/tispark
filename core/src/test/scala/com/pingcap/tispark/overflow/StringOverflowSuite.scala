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
class StringOverflowSuite extends BaseBatchWriteTest("test_data_type_string_overflow") {

  test("Test UTF-8 like emoji not overflow") {

    compareTiDBWriteWithJDBC {
      case (writeFunc, _) =>
        jdbcUpdate(s"drop table if exists $dbtable")
        jdbcUpdate(s"create table $dbtable(i int, c1 CHAR(1))")
        val row = Row(1, "\uD83D\uDE09")
        val schema =
          StructType(List(StructField("i", IntegerType), StructField("c1", StringType)))
        writeFunc(List(row), schema, None)
        compareTiDBSelectWithJDBC(List(row), schema)
    }
  }

  test("Test UTF-8 like emoji overflow") {

    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"create table $dbtable(c1 CHAR(1))")
    val row = Row("\uD83D\uDE09\uD83D\uDE09")
    val schema =
      StructType(List(StructField("c1", StringType)))

    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value \uD83D\uDE09\uD83D\uDE09 length > max length 1"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test CHAR Overflow") {
    testCharOverflow(false)
  }

  test("Test CHAR as key Overflow") {
    testCharOverflow(true)
  }

  test("Test VARCHAR Overflow") {
    testVarcharOverflow(false)
  }

  test("Test VARCHAR as key Overflow") {
    testVarcharOverflow(true)
  }

  private def testCharOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 CHAR(8) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 CHAR(8))")
    }

    val row = Row("123456789")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test TINYTEXT Overflow") {
    testTinyTextOverflow(false)
  }

  test("Test TINYTEXT as key Overflow") {
    testTinyTextOverflow(true)
  }

  private def testVarcharOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 VARCHAR(8) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 VARCHAR(8))")
    }

    val row = Row("123456789")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  // length is 255 for type TEXT(8) in tidb master branch
  ignore("Test TEXT Overflow") {
    testTextOverflow(false)
  }

  // length is 255 for type TEXT(8) in tidb master branch
  ignore("Test TEXT as key Overflow") {
    testTextOverflow(true)
  }

  private def testTinyTextOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 TINYTEXT, primary key (c1(4)))")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 TINYTEXT)")
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
    val tidbErrorMsg = s"value $str length > max length 255"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  private def testTextOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 TEXT(8), primary key (c1(4)))")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 TEXT(8))")
    }

    val row = Row("123456789")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 123456789 length > max length 8"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }
}
