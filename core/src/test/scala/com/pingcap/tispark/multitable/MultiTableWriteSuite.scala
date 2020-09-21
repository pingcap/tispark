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

package com.pingcap.tispark.multitable

import com.pingcap.tispark.write.{DBTable, TiBatchWrite}
import org.apache.spark.sql._

class MultiTableWriteSuite extends BaseTiSparkEnableBatchWriteTest {

  private val tables =
    "CUSTOMER" ::
      "NATION" ::
      Nil
  private val batchWriteTablePrefix = "BATCH.WRITE"
  private var database: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    database = tpchDBName
    setCurrentDatabase(database)
    for (table <- tables) {
      val tableToWrite = s"${batchWriteTablePrefix}_$table"
      tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      tidbStmt.execute(s"create table if not exists `$tableToWrite` like $table ")
    }
  }

  test("multi table batch write") {
    val data = tables.map { table =>
      val tableToWrite = s"${batchWriteTablePrefix}_$table"
      val dbTable = DBTable(database, tableToWrite)
      val df = sql(s"select * from $table")
      (dbTable, df)
    }.toMap

    // multi table batch write
    TiBatchWrite.write(
      data,
      spark,
      tidbOptions ++ Map("multiTables" -> "true", "isTest" -> "true"))

    for (table <- tables) {
      val tableToWrite = s"${batchWriteTablePrefix}_$table"
      // select
      queryTiDBViaJDBC(s"select * from `$tableToWrite`")

      // assert
      val originCount =
        queryTiDBViaJDBC(s"select count(*) from $table").head.head.asInstanceOf[Long]
      val count =
        queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
      assert(count == originCount)
    }
  }

  override def afterAll(): Unit =
    try {
      setCurrentDatabase(database)
      for (table <- tables) {
        val tableToWrite = s"${batchWriteTablePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }
    } finally {
      super.afterAll()
    }
}
