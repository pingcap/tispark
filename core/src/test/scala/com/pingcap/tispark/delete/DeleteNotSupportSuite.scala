/*
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.delete

import com.pingcap.tispark.datasource.BaseBatchWriteTest
import org.apache.spark.sql.AnalysisException
import org.scalatest.Matchers.{convertToAnyShouldWrapper, have, include, the}

/**
 * Delete not support
 * 1.Delete without WHERE clause or with alwaysTrue WHERE clause
 * 2.Delete with subquery
 * 3.Delete from partition table
 * 4.Delete with Pessimistic Transaction Mode (no test)
 */
class DeleteNotSupportSuite extends BaseBatchWriteTest("test_delete_not_support") {

  test("Delete without WHERE clause") {
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")

    the[IllegalArgumentException] thrownBy {
      spark.sql(s"delete from $dbtable")
    } should have message "requirement failed: Delete without WHERE clause is not supported"
  }

  test("Delete with alwaysTrue WHERE clause") {
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")

    the[IllegalArgumentException] thrownBy {
      spark.sql(s"delete from $dbtable where 1=1")
    } should have message "requirement failed: Delete with alwaysTrue WHERE clause is not supported"
  }

  test("Delete with subquery") {
    jdbcUpdate(s"create table $dbtable(i int, s int,PRIMARY KEY (i))")

    intercept[AnalysisException] {
      spark.sql(s"delete from $dbtable where i in (select i from $dbtable)")
    }.getMessage() should include("Delete by condition with subquery is not supported")
  }
}
