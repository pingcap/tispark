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
import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * DATE type include:
 * 1. DATE
 */
class DateOverflowSuite extends BaseDataSourceTest("test_data_type_date_overflow") {

  test("Test DATE YEAR Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testYearOverflow(false)
  }

  test("Test DATE as key YEAR Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testYearOverflow(true)
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }

  test("Test DATE Month Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testMonthOverflow(false)
  }

  test("Test DATE as key Month Upper bound Overflow") {
    if (!supportBatchWrite) {
      cancel
    }
    testMonthOverflow(true)
  }

  private def testYearOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 DATE primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 DATE)")
    }

    val row = Row("10000-01-01")
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

  private def testMonthOverflow(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 DATE primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 DATE)")
    }

    val row = Row("2019-13-01")
    val schema = StructType(List(StructField("c1", StringType)))
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val tidbErrorClass = classOf[TiDBConvertException]
    val tidbErrorMsgStart = null

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      tidbErrorClass,
      tidbErrorMsgStart,
      msgStartWith = true)
  }
}
