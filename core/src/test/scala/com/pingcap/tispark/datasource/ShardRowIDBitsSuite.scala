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
import org.apache.spark.sql.types._

class ShardRowIDBitsSuite extends BaseBatchWriteTest("test_datasource_shard_row_id_bits") {
  private val schema = StructType(List(StructField("i", LongType)))

  test("tispark overflow") {
    val maxShardRowIDBits = 15

    jdbcUpdate(s"create table $dbtable(i int) SHARD_ROW_ID_BITS = $maxShardRowIDBits")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 64 - maxShardRowIDBits - 1).toLong - 3
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // TiSpark insert overflow

    val caught = intercept[com.pingcap.tikv.exception.AllocateRowIDOverflowException] {
      tidbWrite((1L to 1L).map(Row(_)).toList, schema)
    }
    assert(caught.getMessage.startsWith("Overflow when allocating row id"))
  }

  test("tidb overflow") {
    val maxShardRowIDBits = 15

    jdbcUpdate(s"create table $dbtable(i int) SHARD_ROW_ID_BITS = $maxShardRowIDBits")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 64 - maxShardRowIDBits - 1).toLong - 3
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable values(1)")

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable values(1)")
    }
    assert(caught.getMessage.equals("Failed to read auto-increment value from storage engine"))
  }

  test("test rowid overlap: tidb write -> tispark write -> tidb write") {
    val maxShardRowIDBits = 0

    jdbcUpdate(s"create table $dbtable(i int) SHARD_ROW_ID_BITS = $maxShardRowIDBits")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable values(1)")

    // TiSpark insert
    tidbWrite((2L to 2L).map(Row(_)).toList, schema)
    printTableWithTiDBRowID()
    val tidbRowID = queryTiDBViaJDBC(s"select _tidb_rowid, i from $dbtable where i = 2").head.head
      .asInstanceOf[Long]

    // TiDB insert
    generateDataViaTiDB(tidbRowID + 10000, 3L)
    printTableWithTiDBRowID(where = "where i = 2")

    // assert
    val tidbRowID2 = queryTiDBViaJDBC(
      s"select _tidb_rowid, i from $dbtable where i = 2").head.head.asInstanceOf[Long]
    assert(tidbRowID == tidbRowID2)
  }

  private def printTableWithTiDBRowID(where: String = ""): Unit = {
    queryTiDBViaJDBC(s"select _tidb_rowid, i from $dbtable $where order by i").foreach { row =>
      print(getLongBinaryString(row.head.asInstanceOf[Long]))
      println("\t" + row)
    }
  }

  private def getTableCount: Long = {
    queryTiDBViaJDBC(s"select count(*) from $dbtable").head.head.asInstanceOf[Long]
  }

  private def generateDataViaTiDB(minCount: Long, value: Long): Unit = {
    (1 to 10).foreach(i => jdbcUpdate(s"insert into $dbtable values($value)"))

    while (getTableCount < minCount) {
      jdbcUpdate(s"insert into $dbtable select * from $dbtable where i = $value limit 200000")
    }
  }
}
