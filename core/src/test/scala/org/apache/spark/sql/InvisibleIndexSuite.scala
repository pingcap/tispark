/*
 * Copyright 2022 PingCAP, Inc.
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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.BasePlanTest
import org.scalatest.exceptions.TestFailedException

class InvisibleIndexSuite extends BasePlanTest {

  test("test invisible index in catalog") {
    if (!supportInvisibleIndex) {
      cancel("current version of TiDB does not support invisible index!")
    }

    tidbStmt.execute("drop table if exists t_invisible_index")
    tidbStmt.execute("create table t_invisible_index(a int, index idx_a(a))")
    val tiTableInfo1 =
      ti.clientSession.getCatalog.getTable(dbPrefix + "tispark_test", "t_invisible_index")
    assert(!tiTableInfo1.getIndices(true).get(0).isInvisible)

    tidbStmt.execute("alter table t_invisible_index alter index idx_a invisible")
    val tiTableInfo2 =
      ti.clientSession.getCatalog.getTable(dbPrefix + "tispark_test", "t_invisible_index")
    assert(tiTableInfo2.getIndices(true).get(0).isInvisible)
  }

  test("test invisible index in planner") {
    if (!supportInvisibleIndex) {
      cancel("current version of TiDB does not support invisible index!")
    }

    {
      tidbStmt.execute("drop table if exists t_invisible_index")
      tidbStmt.execute("create table t_invisible_index(a int, b int, index idx_a(a))")

      tidbStmt.execute(
        "insert into t_invisible_index values(1, 1),(2, 2),(3, 3),(4, 4),(5, 5),(6, 6)")
      tidbStmt.execute("analyze table t_invisible_index")
      val df = spark.sql("select * from t_invisible_index where a = 1")
      checkIsIndexLookUp(df, "t_invisible_index")
      checkIndex(df, "idx_a")
    }

    {
      tidbStmt.execute("drop table if exists t_invisible_index")
      tidbStmt.execute("create table t_invisible_index(a int, b int, index idx_a(a))")
      tidbStmt.execute("alter table t_invisible_index alter index idx_a invisible")

      tidbStmt.execute(
        "insert into t_invisible_index values(1, 1),(2, 2),(3, 3),(4, 4),(5, 5),(6, 6)")
      tidbStmt.execute("analyze table t_invisible_index")
      val df = spark.sql("select * from t_invisible_index where a = 1")
      intercept[TestFailedException] {
        checkIsIndexLookUp(df, "t_invisible_index")
        checkIndex(df, "idx_a")
      }

    }
  }

  private lazy val supportInvisibleIndex: Boolean = {
    var result = true
    tidbStmt.execute("drop table if exists t_invisible_index")
    tidbStmt.execute("create table t_invisible_index(a int, index idx_a(a))")
    try {
      tidbStmt.execute("alter table t_invisible_index alter index idx_a invisible")

      val tiTableInfo =
        ti.clientSession.getCatalog.getTable(dbPrefix + "tispark_test", "t_invisible_index")
      result = tiTableInfo.getIndices(true).get(0).isInvisible
    } catch {
      case e: Throwable => result = false
    }
    result
  }

  override def afterAll(): Unit = {
    try {
      tidbStmt.execute("drop table if exists t_invisible_index")
    } finally {
      super.afterAll()
    }
  }
}
