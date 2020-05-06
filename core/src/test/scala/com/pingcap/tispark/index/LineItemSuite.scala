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

package com.pingcap.tispark.index

import com.pingcap.tispark.{TiBatchWrite, TiDBOptions}
import org.apache.spark.sql.BaseTiSparkTest

class LineItemSuite extends BaseTiSparkTest {

  private var database: String = _

  private val table = "LINEITEM"

  private val limit = 30000

  private val batchWriteTablePrefix = "BATCH.WRITE"

  override def beforeAll(): Unit = {
    super.beforeAll()
    database = tpchDBName
    setCurrentDatabase(database)
    val tableToWrite = s"${batchWriteTablePrefix}_$table"
    tidbStmt.execute(s"drop table if exists `$tableToWrite`")
    tidbStmt.execute(s"create table if not exists `$tableToWrite` like $table ")
  }

  test("ti batch write: lineitem") {
    logDebug(s"start test table [$table]")

    val tableToWrite = s"${batchWriteTablePrefix}_$table"

    // select
    refreshConnections(TestTables(database, tableToWrite))
    val df = sql(s"select * from $table limit $limit")

    // batch write
    TiBatchWrite.writeToTiDB(
      df,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true")
      )
    )

    // refresh
    refreshConnections(TestTables(database, tableToWrite))
    setCurrentDatabase(database)

    // select
    queryTiDBViaJDBC(s"select * from `$tableToWrite`")

    // assert
    // cannot use count since batch write is not support index writing yet.
    val count = queryViaTiSpark(s"select * from `$tableToWrite`").length
      .asInstanceOf[Long]
    assert(count == limit)
  }

  override def afterAll(): Unit =
    try {
      setCurrentDatabase(database)
      val tableToWrite = s"${batchWriteTablePrefix}_$table"
      tidbStmt.execute(s"drop table if exists `$tableToWrite`")

    } finally {
      super.afterAll()
    }
}
