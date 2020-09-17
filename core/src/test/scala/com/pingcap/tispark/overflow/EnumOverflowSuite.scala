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
 * ENUM type include:
 * 1. ENUM
 */
class EnumOverflowSuite extends BaseBatchWriteTest("test_data_type_enum_overflow") {

  test("Test ENUM Value Overflow") {
    testEnumValueOverflow(false)
  }

  test("Test ENUM as key Value Overflow") {
    testEnumValueOverflow(true)
  }

  test("Test ENUM Number Overflow") {
    testEnumNumberOverflow(false)
  }

  test("Test ENUM as key Number Overflow") {
    testEnumNumberOverflow(true)
  }

  private def testEnumValueOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown') primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown'))")
    }

    val row = Row("abc")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "Incorrect enum value: 'abc'"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }

  private def testEnumNumberOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown') primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 ENUM('male', 'female', 'both', 'unknown'))")
    }

    val row = Row("5")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 5 > upperBound 4"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsg)
  }
}
