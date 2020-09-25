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

class SequenceTestSuite extends BaseTiSparkTest {
  private val table = "test_seq"

  test("Test seq") {
    dropTbl()

    tidbStmt.execute(s"create table $table(qty INT, price INT);")
    tidbStmt.execute(s"INSERT INTO $table VALUES(3, 50);")

    try {
      tidbStmt.execute(s"CREATE sequence sq_test;")
    } catch {
      case _: Exception => cancel
    }

    refreshConnections()

    judge(s"select * from $table")

    spark.sql("show tables").show(false)

    val tableInfo = ti.meta.getTable(s"${dbPrefix}tispark_test", table).get
    assert(table.equals(tableInfo.getName))

    val sequenceInfo = ti.meta.getTable(s"${dbPrefix}tispark_test", "sq_test")
    assert(sequenceInfo.isEmpty)
  }

  override def afterAll(): Unit = {
    tidbStmt.execute(s"drop table if exists $table")
    try {
      tidbStmt.execute("drop sequence if exists sq_test")
    } catch {
      case _: Exception =>
    }
  }

  private def dropTbl() = {
    tidbStmt.execute(s"drop table if exists $table")
    try {
      tidbStmt.execute("drop sequence if exists sq_test")
    } catch {
      case _: Exception => cancel
    }
  }
}
