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

package org.apache.spark.sql

class ViewTestSuite extends BaseTiSparkTest {
  private val table = "test_view"

  test("Test View") {
    dropTbl()

    tidbStmt.execute(s"create table $table(qty INT, price INT);")
    tidbStmt.execute(s"INSERT INTO $table VALUES(3, 50);")

    try {
      tidbStmt.execute(s"CREATE VIEW v AS SELECT qty, price, qty*price AS value FROM $table;")
    } catch {
      case _: Exception => cancel
    }

    refreshConnections()

    judge(s"select * from $table")
    intercept[AnalysisException](spark.sql("select * from v"))

    spark.sql("show tables").show(false)
  }

  override def afterAll(): Unit = {
    dropTbl()
  }

  private def dropTbl() = {
    tidbStmt.execute(s"drop table if exists $table")
    try {
      tidbStmt.execute("drop view if exists v")
    } catch {
      case _: Exception => cancel
    }
  }
}
