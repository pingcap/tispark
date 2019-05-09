/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tispark

import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

class TiBatchWriteSuite extends BaseTiSparkSuite {

  private val database = "tpch_test"

  private val tables =
    "CUSTOMER" ::
      //"LINEITEM" no primary key, current not support
      "NATION" ::
      "ORDERS" ::
      "PART" ::
      //"PARTSUPP" no primary key, current not support
      "REGION" ::
      "SUPPLIER" ::
      Nil

  private val batchWriteTablePrefix = "BATCH_WRITE"

  override def beforeAll(): Unit = {
    super.beforeAll()
    setCurrentDatabase(database)
    for (table <- tables) {
      tidbStmt.execute(s"drop table if exists ${batchWriteTablePrefix}_$table")
      tidbStmt.execute(s"create table if not exists ${batchWriteTablePrefix}_$table like $table ")
    }
  }

  test("ti batch write") {

    for (table <- tables) {
      // select
      refreshConnections(TestTables(database, s"${batchWriteTablePrefix}_$table"))
      val df = sql(s"select * from $table")

      // batch write
      TiBatchWrite.writeToTiDB(
        df.rdd,
        ti,
        new TiDBOptions(
          tidbOptions + ("database" -> s"$dbPrefix$database", "table" -> s"${batchWriteTablePrefix}_$table")
        )
      )

      // refresh
      refreshConnections(TestTables(database, s"${batchWriteTablePrefix}_$table"))
      setCurrentDatabase(database)

      // select
      tidbStmt.execute(s"select * from ${batchWriteTablePrefix}_$table")

      // assert
      val originCount = querySpark(s"select count(*) from $table").head.head.asInstanceOf[Long]
      // cannot use count since batch write is not support index writing yet.
      val count = querySpark(s"select * from ${batchWriteTablePrefix}_$table").length
        .asInstanceOf[Long]
      assert(count == originCount)
    }
  }

  test("table not exists") {
    // select
    val df = sql(s"select * from CUSTOMER")

    // batch write
    intercept[NoSuchTableException] {
      TiBatchWrite.writeToTiDB(
        df.rdd,
        ti,
        new TiDBOptions(
          tidbOptions + ("database" -> s"$dbPrefix$database", "table" -> s"${batchWriteTablePrefix}_TABLE_NOT_EXISTS")
        )
      )
    }
  }

  override def afterAll(): Unit =
    try {
      setCurrentDatabase(database)
      for (table <- tables) {
        tidbStmt.execute(s"drop table if exists ${batchWriteTablePrefix}_$table")
      }
    } finally {
      super.afterAll()
    }
}
