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

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

class BaseBatchWriteWithoutDropTableTest(
    override val table: String,
    override val database: String = "tispark_test")
    extends BaseDataSourceTest(table, database) {

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (!supportBatchWrite) {
      cancel
    }
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }

  protected def compareTiDBWriteWithJDBC(
      testCode: ((List[Row], StructType, Option[Map[String, String]]) => Unit, String) => Unit)
      : Unit = {
    testCode(tidbWrite, "tidbWrite")
    testCode(jdbcWrite, "jdbcWrite")
  }

  protected def compareTiDBSelectWithJDBC_V2(sortCol: String = "i"): Unit = {
    compareTiDBSelectWithJDBCWithTable_V2(table, sortCol)
  }

  protected def compareTiDBSelectWithJDBCWithTable_V2(
      tblName: String,
      sortCol: String = "i"): Unit = {
    val sql = s"select * from `$database`.`$tblName` order by $sortCol"

    // check jdbc result & data source result
    val jdbcResult = queryTiDBViaJDBC(sql)
    val df = queryDatasourceTiDBWithTable(sortCol, tableName = tblName)
    val tidbResult = seqRowToList(df.collect(), df.schema)

    if (!compResult(jdbcResult, tidbResult)) {
      logger.error(s"""Failed on $tblName\n
                      |DataSourceAPI result: ${listToString(jdbcResult)}\n
                      |TiDB via JDBC result: ${listToString(tidbResult)}""".stripMargin)
      fail()
    }
  }
}
