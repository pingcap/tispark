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

class FilterPushdownSuite
    extends BaseBatchWriteWithoutDropTableTest("test_datasource_filter_pushdown") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  override def beforeAll(): Unit = {
    super.beforeAll()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB'), (3, 'Spark'), (4, null)")
  }

  test("Test Simple Comparisons") {
    testTiDBSelectFilter("s = 'Hello'", Seq(row1))
    testTiDBSelectFilter("i > 2", Seq(row3, row4))
    testTiDBSelectFilter("i < 3", Seq(row2))
  }

  test("Test >= and <=") {
    testTiDBSelectFilter("i >= 2", Seq(row2, row3, row4))
    testTiDBSelectFilter("i <= 3", Seq(row2, row3))
  }

  test("Test logical operators") {
    testTiDBSelectFilter("i >= 2 AND i <= 3", Seq(row2, row3))
    testTiDBSelectFilter("NOT i = 3", Seq(row2, row4))
    testTiDBSelectFilter("NOT i = 3 OR i IS NULL", Seq(row1, row2, row4))
    testTiDBSelectFilter("i IS NULL OR i > 2 AND s IS NOT NULL", Seq(row1, row3))
  }

  test("Test IN") {
    testTiDBSelectFilter("i IN ( 2, 3)", Seq(row2, row3))
  }
}
