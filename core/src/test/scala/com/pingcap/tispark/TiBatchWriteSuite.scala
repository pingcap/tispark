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

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

class TiBatchWriteSuite extends BaseTiSparkTest {

  private var database: String = _

  private val tables =
    "CUSTOMER" ::
      //"LINEITEM" :: to large for test
      "NATION" ::
      "ORDERS" ::
      "PART" ::
      "PARTSUPP" ::
      "REGION" ::
      "SUPPLIER" ::
      Nil

  private val batchWriteTablePrefix = "BATCH.WRITE"

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

  test("test") {
    tidbStmt.execute("DROP TABLE IF EXISTS test.ISHBZH")
    tidbStmt.execute("DROP TABLE IF EXISTS test.ISHBZH2")
    tidbStmt.execute(
      """
        |create table test.ISHBZH
        |(
        |ZQZH char(20) default '' not null,
        |MAIN char(20) default '' not null,
        |ZHBZ char default '' not null,
        |GDLB char default '' not null,
        |CLRQ date default '2001-01-01 00:00:00' not null,
        |CLSJ time default '00:00:00' not null,
        |GLSL decimal(5) default '0' not null,
        |BYZD char(20) default '' not null,
        |constraint ZQZH unique (ZQZH),
        |constraint ZHBZ unique (ZHBZ, MAIN, ZQZH)
        |)
      """.stripMargin
    )

    tidbStmt.execute(
      """
        |create table test.ISHBZH2
        |(
        |ZQZH char(20) default '' not null,
        |MAIN char(20) default '' not null,
        |ZHBZ char default '' not null,
        |GDLB char default '' not null,
        |CLRQ date default '2001-01-01 00:00:00' not null,
        |CLSJ time default '00:00:00' not null,
        |GLSL decimal(5) default '0' not null,
        |BYZD char(20) default '' not null,
        |constraint ZQZH unique (ZQZH),
        |constraint ZHBZ unique (ZHBZ, MAIN, ZQZH)
        |)
      """.stripMargin
    )

    //second * 1000000000 + minnute * 60000000000 + hour * 3600000000000
    tidbStmt.execute("""insert into test.ISHBZH(ZQZH, CLSJ) values
                       |('1', '00:00:01'), ('2', '00:01:00'), ('3', '01:00:00'),
                       |('4', '00:00:02'), ('5', '00:02:00'), ('6', '02:00:00')
                       |"""".stripMargin)

    val df = spark.sql("select * from tidb_test.ISHBZH")

    df.printSchema()
    df.show(false)

    TiBatchWrite.writeToTiDB(
      df,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> "test", "table" -> "ISHBZH2", "replace" -> "true")
      )
    )
  }

  test("ti batch write") {
    for (table <- tables) {
      val tableToWrite = s"${batchWriteTablePrefix}_$table"

      // select
      refreshConnections(TestTables(database, tableToWrite))
      val df = sql(s"select * from $table")

      // batch write
      TiBatchWrite.writeToTiDB(
        df,
        ti,
        new TiDBOptions(
          tidbOptions + ("database" -> s"$database", "table" -> tableToWrite)
        )
      )

      // refresh
      refreshConnections(TestTables(database, tableToWrite))
      setCurrentDatabase(database)

      // select
      queryTiDBViaJDBC(s"select * from `$tableToWrite`")

      // assert
      val originCount = queryViaTiSpark(s"select count(*) from $table").head.head.asInstanceOf[Long]
      // cannot use count since batch write is not support index writing yet.
      val count = queryViaTiSpark(s"select * from `$tableToWrite`").length
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
        df,
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
        val tableToWrite = s"${batchWriteTablePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }
    } finally {
      super.afterAll()
    }
}
