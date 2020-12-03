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

import com.pingcap.tikv.exception.{ConvertOverflowException, TiBatchWriteException}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AutoRandomSuite extends BaseBatchWriteTest("test_datasource_auto_random") {
  test("auto random column insert null") {
    if (!supportAutoRandom) {
      cancel("current version of TiDB does not support auto random or is disabled!")
    }

    val row = Row(null)
    val schema = StructType(List(StructField("i", LongType)))

    jdbcUpdate(s"create table $dbtable(i bigint primary key NOT NULL AUTO_RANDOM)")

    val caught = intercept[TiBatchWriteException] {
      tidbWrite(List(row), schema)
    }
    assert(
      caught.getMessage.equals(
        "tispark currently does not support write data to table with auto random column!"))
  }

  test("auto random column insert not null") {
    if (!supportAutoRandom) {
      cancel("current version of TiDB does not support auto random or is disabled!")
    }

    val row = Row(1L)
    val schema = StructType(List(StructField("i", LongType)))

    jdbcUpdate(s"create table $dbtable(i bigint primary key NOT NULL AUTO_RANDOM)")

    val caught = intercept[TiBatchWriteException] {
      tidbWrite(List(row), schema)
    }
    assert(
      caught.getMessage.equals(
        "tispark currently does not support write data to table with auto random column!"))
  }

  protected lazy val supportAutoRandom: Boolean = {
    var result = true
    tidbStmt.execute("drop table if exists t")
    try {
      jdbcUpdate(s"create table t(i bigint primary key NOT NULL AUTO_RANDOM)")
    } catch {
      case e: Throwable => result = false
    } finally {
      tidbStmt.execute("drop table if exists t")
    }
    result
  }
}
