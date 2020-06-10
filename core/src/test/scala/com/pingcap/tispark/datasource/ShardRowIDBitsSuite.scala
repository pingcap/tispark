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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class ShardRowIDBitsSuite extends BaseDataSourceTest("test_shard_row_id_bits") {
  private val row1 = Row(1)
  private val row2 = Row(2)
  private val row3 = Row(3)
  private val schema = StructType(List(StructField("a", IntegerType)))

  test("reading and writing a table with shard_row_id_bits") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"CREATE TABLE  $dbtable ( `a` int(11))  SHARD_ROW_ID_BITS = 4")

    jdbcUpdate(s"insert into $dbtable values(null)")

    tidbWrite(List(row1, row2, row3), schema)
    testTiDBSelect(List(Row(null), row1, row2, row3), sortCol = "a")
  }
}
