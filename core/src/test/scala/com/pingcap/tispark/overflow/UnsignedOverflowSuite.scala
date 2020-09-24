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

import com.pingcap.tikv.exception.TiDBConvertException
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
class UnsignedOverflowSuite extends BaseBatchWriteTest("test_data_type_unsigned_overflow") {

  test("Test TINYINT UNSIGNED Upper bound Overflow") {
    testTinyIntUnsignedUpperBound(false)
  }

  test("Test TINYINT UNSIGNED as key Upper bound Overflow") {
    testTinyIntUnsignedUpperBound(true)
  }

  test("Test TINYINT UNSIGNED Lower bound Overflow") {
    testTinyIntUnsignedLowerBound(false)
  }

  test("Test TINYINT UNSIGNED as key Lower bound Overflow") {
    testTinyIntUnsignedLowerBound(false)
  }

  private def testTinyIntUnsignedUpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 TINYINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 TINYINT UNSIGNED)")
    }

    val row = Row(256)
    val schema = StructType(List(StructField("c1", IntegerType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 256 > upperBound 255"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test SMALLINT UNSIGNED Upper bound Overflow") {
    testSmallIntUnsignedUpperBound(false)
  }

  test("Test SMALLINT UNSIGNED as key Upper bound Overflow") {
    testSmallIntUnsignedUpperBound(true)
  }

  private def testTinyIntUnsignedLowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 TINYINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 TINYINT UNSIGNED)")
    }

    val row = Row(-1)
    val schema = StructType(List(StructField("c1", IntegerType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test SMALLINT UNSIGNED Lower bound Overflow") {
    testSmallIntUnsignedLowerBound(false)
  }

  test("Test SMALLINT UNSIGNED as key Lower bound Overflow") {
    testSmallIntUnsignedLowerBound(true)
  }

  private def testSmallIntUnsignedUpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 SMALLINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 SMALLINT UNSIGNED)")
    }

    val row = Row(65536)
    val schema = StructType(List(StructField("c1", IntegerType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 65536 > upperBound 65535"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test MEDIUMINT UNSIGNED Upper bound Overflow") {
    testMediumIntUnsignedUpperBound(false)
  }

  test("Test MEDIUMINT UNSIGNED as key Upper bound Overflow") {
    testMediumIntUnsignedUpperBound(true)
  }

  private def testSmallIntUnsignedLowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 SMALLINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 SMALLINT UNSIGNED)")
    }

    val row = Row(-1)
    val schema = StructType(List(StructField("c1", IntegerType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test MEDIUMINT UNSIGNED Lower bound Overflow") {
    testMediumIntUnsignedLowerBound(false)
  }

  test("Test MEDIUMINT UNSIGNED as key Lower bound Overflow") {
    testMediumIntUnsignedLowerBound(true)
  }

  private def testMediumIntUnsignedUpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 MEDIUMINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 MEDIUMINT UNSIGNED)")
    }

    val row = Row(16777216)
    val schema = StructType(List(StructField("c1", IntegerType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 16777216 > upperBound 16777215"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test INT UNSIGNED Upper bound Overflow") {
    testIntUnsignedUpperBound(false)
  }

  test("Test INT UNSIGNED as key Upper bound Overflow") {
    testIntUnsignedUpperBound(true)
  }

  private def testMediumIntUnsignedLowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 MEDIUMINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 MEDIUMINT UNSIGNED)")
    }

    val row = Row(-1)
    val schema = StructType(List(StructField("c1", IntegerType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test INT UNSIGNED Lower bound Overflow") {
    testIntUnsignedLowerBound(false)
  }

  test("Test INT UNSIGNED as key Lower bound Overflow") {
    testIntUnsignedLowerBound(true)
  }

  private def testIntUnsignedUpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 INT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 INT UNSIGNED)")
    }

    val row = Row(4294967296L)
    val schema = StructType(List(StructField("c1", LongType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 4294967296 > upperBound 4294967295"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  test("Test BIGINT UNSIGNED Upper bound Overflow") {
    testBigIntUnsignedUpperBound(false)
  }

  test("Test BIGINT UNSIGNED as key Upper bound Overflow") {
    testBigIntUnsignedUpperBound(true)
  }

  private def testIntUnsignedLowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 INT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 INT UNSIGNED)")
    }

    val row = Row(-1)
    val schema = StructType(List(StructField("c1", LongType)))
    val jdbcErrorClass = classOf[java.lang.RuntimeException]
    val tidbErrorClass = classOf[java.lang.RuntimeException]
    val tidbErrorMsgStartWith =
      "Error while encoding: java.lang.RuntimeException: java.lang.Integer is not a valid external type for schema of bigint\nif (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 0, c1), LongType) AS c1"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsgStartWith,
      msgStartWith = true)
  }

  test("Test BIGINT UNSIGNED Lower bound Overflow") {
    testBigIntUnsignedLowerBound(false)
  }

  test("Test BIGINT UNSIGNED as key Lower bound Overflow") {
    testBigIntUnsignedLowerBound(true)
  }

  private def testBigIntUnsignedUpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIGINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIGINT UNSIGNED)")
    }

    val row = Row("18446744073709551616")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[TiDBConvertException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  private def testBigIntUnsignedLowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIGINT UNSIGNED primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIGINT UNSIGNED)")
    }

    val row = Row("-1")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[TiDBConvertException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }
}
