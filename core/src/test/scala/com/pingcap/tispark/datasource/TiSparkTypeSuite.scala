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
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class TiSparkTypeSuite extends BaseBatchWriteTest("type_test") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2L, "TiDB")
  private val row3 = Row(3L, "Spark")
  private val row5 = Row(Long.MaxValue, "Duplicate")

  private val schema = StructType(List(StructField("i", LongType), StructField("s", StringType)))
  test("bigint test") {
    jdbcUpdate(s"create table $dbtable(i bigint, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(null, 'Hello'), (2, 'TiDB')")

    tidbWrite(List(row3, row5), schema)
    testTiDBSelect(List(row1, row2, row3, row5))
  }
}
