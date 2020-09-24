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
import org.apache.spark.sql.types._

/**
 * BIT type include:
 * 1. BIT
 */
class BitOverflowSuite extends BaseBatchWriteTest("test_data_type_bit_overflow") {

  test("Test BIT(1) Upper bound Overflow") {
    testBit1UpperBound(false)
  }
  test("Test BIT(1) as key Upper bound Overflow") {
    testBit1UpperBound(true)
  }

  test("Test BIT(1) Lower bound Overflow") {
    testBit1LowerBound(false)
  }

  test("Test BIT(1) as key Lower bound Overflow") {
    testBit1LowerBound(true)
  }

  private def testBit1UpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIT(1) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIT(1))")
    }

    val row = Row(2.toByte)
    val schema = StructType(List(StructField("c1", ByteType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 2 > upperBound 2"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true)
  }

  test("Test BIT(4) Upper bound Overflow") {
    testBit4UpperBound(false)
  }

  test("Test BIT(4) as key Upper bound Overflow") {
    testBit4UpperBound(true)
  }

  private def testBit1LowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIT(1) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIT(1))")
    }

    val row = Row(-1.toByte)
    val schema = StructType(List(StructField("c1", ByteType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true)
  }

  test("Test BIT(4) Lower bound Overflow") {
    testBit4LowerBound(false)
  }

  test("Test BIT(4) as key Lower bound Overflow") {
    testBit4LowerBound(true)
  }

  private def testBit4UpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIT(4) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIT(4))")
    }

    val row = Row(16.toByte)
    val schema = StructType(List(StructField("c1", ByteType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 16 > upperBound 16"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true)
  }

  test("Test BIT(8) Upper bound Overflow") {
    testBit8UpperBound(false)
  }

  test("Test BIT(8) as key Upper bound Overflow") {
    testBit8UpperBound(true)
  }

  private def testBit4LowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIT(4) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIT(4))")
    }

    val row = Row(-1.toByte)
    val schema = StructType(List(StructField("c1", ByteType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true)
  }

  test("Test BIT(8) Lower bound Overflow") {
    testBit8LowerBound(false)
  }

  test("Test BIT(8) as key Lower bound Overflow") {
    testBit8LowerBound(true)
  }

  private def testBit8UpperBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIT(8) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIT(8))")
    }

    val row = Row(256L)
    val schema = StructType(List(StructField("c1", LongType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 256 > upperBound 256"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true)
  }

  private def testBit8LowerBound(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 BIT(8) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 BIT(8))")
    }

    val row = Row(-1L)
    val schema = StructType(List(StructField("c1", LongType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg,
      msgStartWith = true)
  }
}
