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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

class ShardRowIDBitsSuite extends BaseDataSourceTest("test_shard_row_id_bits") {
  private val row = (0 to 12).map(x => Row(x.toString)).toArray
  private val schema = StructType(List(StructField("a", StringType)))

  test("reading and writing a table with shard_row_id_bits") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"CREATE TABLE $dbtable( `a` varchar(11)) SHARD_ROW_ID_BITS=2 PRE_SPLIT_REGIONS=2")
    jdbcUpdate(s"insert into $dbtable(a) values(null),('0'),('1'),('2'),('3'),('4')")
    tidbWrite(List(row(5), row(6), row(7)), schema)
    jdbcUpdate(s"insert into $dbtable values('8'),('9'),('10'),('11'),('12')")

    testTiDBSelect(List(Row(null)) ++ row.sortBy(r => r.getString(0)), sortCol = "a")
  }
}
