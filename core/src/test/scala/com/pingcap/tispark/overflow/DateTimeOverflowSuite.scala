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
 * DATETIME type include:
 * 1. DATETIME
 */
class DateTimeOverflowSuite extends BaseBatchWriteTest("test_data_type_datetime_overflow") {

  test("Test DATETIME YEAR Overflow") {
    testDateTimeOverflow(false)
  }

  test("Test DATETIME as key YEAR Overflow") {
    testDateTimeOverflow(true)
  }

  private def testDateTimeOverflow(testKey: Boolean): Unit = {
    if (testKey) {
      jdbcUpdate(s"create table $dbtable(c1 DATETIME(6) primary key)")
    } else {
      jdbcUpdate(s"create table $dbtable(c1 DATETIME(6))")
    }

    val row = Row("10000-11-11 11:11:11")
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
