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

class AutoIncrementSuite extends BaseDataSourceTest("test_datasource_auto_increment") {

  override def beforeAll(): Unit =
    super.beforeAll()

  // Duplicate entry '2' for key 'PRIMARY'
  // currently user provided auto increment value is not supported!
  ignore("auto increment: user provide id") {
    if (!supportBatchWrite) {
      cancel
    }

    val row1 = Row(1L, 1L)
    val row2 = Row(2L, 2L)
    val row3 = Row(3L, 3L)
    val row4 = Row(4L, 4L)

    val schema = StructType(List(StructField("i", LongType), StructField("j", LongType)))

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    tidbWrite(List(row2, row3, row4), schema)

    testTiDBSelect(Seq(row1, row2, row3, row4))

    // Duplicate entry '2' for key 'PRIMARY'
    jdbcUpdate(s"insert into $dbtable (j) values(5)")
  }

  test("auto increment: tispark generate id") {
    if (!supportBatchWrite) {
      cancel
    }

    val row2 = Row(2L)
    val row3 = Row(3L)
    val row4 = Row(4L)

    val schema = StructType(List(StructField("j", LongType)))

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    tidbWrite(List(row2, row3, row4), schema)

    jdbcUpdate(s"insert into $dbtable (j) values(5)")

    assert(5 == sql(s"select * from $dbtableWithPrefix").collect().length)
  }

  test("signed tidb overflow") {
    if (!supportBatchWrite) {
      cancel
    }

    val schema = StructType(List(StructField("j", LongType)))

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i bigint NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 63).toLong - 3
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable (j) values(1)")
    }
    assert(caught.getMessage.equals("Failed to read auto-increment value from storage engine"))
  }

  test("signed tispark overflow") {
    if (!supportBatchWrite) {
      cancel
    }

    val schema = StructType(List(StructField("j", LongType)))

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i bigint NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 63).toLong - 3
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiSpark insert overflow
    val caught = intercept[com.pingcap.tikv.exception.TiBatchWriteException] {
      tidbWrite((1L to 1L).map(Row(_)).toList, schema)
    }
    assert(caught.getMessage.equals("cannot allocate ids for this write"))
  }

  test("unsigned tidb overflow") {
    if (!supportBatchWrite) {
      cancel
    }

    val schema = StructType(List(StructField("j", LongType)))

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i bigint unsigned NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 0xfffffffffffffffcL
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable (j) values(1)")
    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable (j) values(1)")
    }
    assert(caught.getMessage.equals("Failed to read auto-increment value from storage engine"))
  }

  test("unsigned tispark overflow") {
    if (!supportBatchWrite) {
      cancel
    }

    val schema = StructType(List(StructField("j", LongType)))

    dropTable()

    jdbcUpdate(
      s"create table $dbtable(i bigint unsigned NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 0xfffffffffffffffcL
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiSpark insert overflow
    val caught = intercept[com.pingcap.tikv.exception.TiBatchWriteException] {
      tidbWrite((1L to 1L).map(Row(_)).toList, schema)
    }
    assert(caught.getMessage.equals("cannot allocate ids for this write"))
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
