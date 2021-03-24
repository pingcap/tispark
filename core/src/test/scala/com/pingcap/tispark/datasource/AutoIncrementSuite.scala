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

import com.pingcap.tikv.exception.ConvertOverflowException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

class AutoIncrementSuite extends BaseBatchWriteTest("test_datasource_auto_increment") {

  test("alter primary key + auto increment + shard row bits") {
    if (!isEnableAlterPrimaryKey) {
      cancel("enable alter-primary-key by changing tidb.toml")
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i)) SHARD_ROW_ID_BITS=4")

    val tiTableInfo = ti.tiSession.getCatalog.getTable(dbPrefix + database, table)
    assert(!tiTableInfo.isPkHandle)

    (1L until 10L).foreach { i =>
      jdbcUpdate(s"insert into $dbtable (j) values(${i * 2 - 1})")

      tidbWrite(List(Row(i * 2)), schema)
    }

    println(listToString(queryTiDBViaJDBC(s"select _tidb_rowid, i, j from $dbtable")))

    spark.sql(s"select * from $table").show

    val maxI = queryTiDBViaJDBC(s"select max(i) from $dbtable").head.head.toString.toLong
    assert(maxI < 10000000)

    val maxTiDBRowID =
      queryTiDBViaJDBC(s"select max(_tidb_rowid) from $dbtable").head.head.toString.toLong
    assert(maxTiDBRowID > 10000000)
  }

  // Duplicate entry '2' for key 'PRIMARY'
  // currently user provided auto increment value is not supported!
  ignore("auto increment: user provide id") {
    val row1 = Row(1L, 1L)
    val row2 = Row(2L, 2L)
    val row3 = Row(3L, 3L)
    val row4 = Row(4L, 4L)

    val schema = StructType(List(StructField("i", LongType), StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    tidbWrite(List(row2, row3, row4), schema)

    testTiDBSelect(Seq(row1, row2, row3, row4))

    // Duplicate entry '2' for key 'PRIMARY'
    jdbcUpdate(s"insert into $dbtable (j) values(5)")
  }

  test("auto increment: tispark generate id") {
    val row2 = Row(2L)
    val row3 = Row(3L)
    val row4 = Row(4L)

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    tidbWrite(List(row2, row3, row4), schema)

    jdbcUpdate(s"insert into $dbtable (j) values(5)")

    assert(5 == sql(s"select * from $dbtableWithPrefix").collect().length)
  }

  test("bigint signed tidb overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i bigint NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 63).toLong - 5
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable (j) values(1),(2),(3)")
    }
    assert(caught.getMessage.equals("Failed to read auto-increment value from storage engine"))
  }

  test("bigint signed tispark overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i bigint NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = Math.pow(2, 63).toLong - 6
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiSpark insert overflow
    intercept[Exception] {
      tidbWrite((1L to 4L).map(Row(_)).toList, schema)
    }
  }

  test("bigint unsigned tidb overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i bigint unsigned NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 0xfffffffffffffffaL
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable (j) values(1)")
    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable (j) values(1),(2),(3)")
    }
    assert(caught.getMessage.equals("Failed to read auto-increment value from storage engine"))
  }

  test("bigint unsigned tispark overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i bigint unsigned NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 0xfffffffffffffff9L
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiSpark insert overflow
    intercept[Exception] {
      tidbWrite((1L to 4L).map(Row(_)).toList, schema)
    }
  }

  test("tinyint signed tidb overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i tinyint NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 124
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable (j) values(1)")

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable (j) values(1),(2)")
    }
    assert(caught.getMessage.startsWith("Data truncation: constant"))
  }

  test("tinyint signed tispark overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i tinyint NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 124
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiSpark insert overflow
    val caught = intercept[org.apache.spark.SparkException] {
      tidbWrite((1L to 2L).map(Row(_)).toList, schema)
    }
    assert(caught.getCause.isInstanceOf[ConvertOverflowException])
  }

  test("tinyint unsigned tidb overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i tinyint unsigned NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 252
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiDB insert
    jdbcUpdate(s"insert into $dbtable (j) values(1)")
    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiDB insert overflow
    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable (j) values(1),(2)")
    }
    assert(caught.getMessage.startsWith("Data truncation: constant"))
  }

  test("tinyint unsigned tispark overflow") {
    if (isEnableAlterPrimaryKey) {
      cancel()
    }

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i tinyint unsigned NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    // hack: update AllocateId on TiKV to a huge number to trigger overflow
    val size = 252
    val allocator = allocateID(size)
    println(s"start: \t${getLongBinaryString(allocator.getStart)}")
    println(s"end: \t${getLongBinaryString(allocator.getEnd)}")

    // TiSpark insert
    tidbWrite((1L to 1L).map(Row(_)).toList, schema)

    sql(s"select * from $dbtableWithPrefix").show(false)

    // TiSpark insert overflow
    val caught = intercept[org.apache.spark.SparkException] {
      tidbWrite((1L to 2L).map(Row(_)).toList, schema)
    }
    assert(caught.getCause.isInstanceOf[ConvertOverflowException])
  }

  test("auto increment: user provide id, update") {
    val row1 = Row(1L, 1L)
    val row2 = Row(2L, 22L)
    val row3 = Row(3L, 33L)
    val row4 = Row(4L, 44L)

    val schema = StructType(List(StructField("i", LongType), StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i), key (j))")

    jdbcUpdate(s"insert into $dbtable (j) values(1), (2), (3), (4)")

    sql(s"select * from $dbtableWithPrefix").show()

    tidbWrite(List(row2, row3, row4), schema, Some(Map("replace" -> "true")))

    sql(s"select * from $dbtableWithPrefix").show()

    testTiDBSelect(Seq(row1, row2, row3, row4))
  }

  test("auto increment: user provide id, update+insert") {
    val row1 = Row(1L, 1L)
    val row2 = Row(2L, 22L)
    val row3 = Row(3L, 33L)
    val row4 = Row(4L, 44L)

    val schema = StructType(List(StructField("i", LongType), StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, primary key (i))")

    jdbcUpdate(s"insert into $dbtable (j) values(1), (2), (3)")

    sql(s"select * from $dbtableWithPrefix").show()

    val caught = intercept[com.pingcap.tikv.exception.TiBatchWriteException] {
      tidbWrite(List(row2, row3, row4), schema, Some(Map("replace" -> "true")))
    }
    assert(
      caught.getMessage.equals(
        "currently user provided auto increment value is only supported in update mode!"))
  }

  test("auto increment but not primary key") {
    val row3 = Row(3L, 33L)
    val row4 = Row(4L, 44L)

    val schema = StructType(List(StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, unique key (i))")

    jdbcUpdate(s"insert into $dbtable (j) values(1), (2)")

    sql(s"select * from $dbtableWithPrefix").show()

    tidbWrite(List(row3, row4), schema)

    sql(s"select * from $dbtableWithPrefix").show()
  }

  test("auto increment column name compare") {
    val row3 = Row(3L, 33L)
    val row4 = Row(4L, 44L)

    val schema = StructType(List(StructField("I", LongType), StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, unique key (i))")

    jdbcUpdate(s"insert into $dbtable values (3, 0), (4, 0)")

    sql(s"select * from $dbtableWithPrefix").show()

    tidbWrite(List(row3, row4), schema, Some(Map("replace" -> "true")))

    sql(s"select * from $dbtableWithPrefix").show()
  }

  test("auto increment column insert null") {
    val row3 = Row(null, 33L)
    val row4 = Row(null, 44L)

    val schema = StructType(List(StructField("i", LongType), StructField("j", LongType)))

    jdbcUpdate(
      s"create table $dbtable(i int NOT NULL AUTO_INCREMENT, j int NOT NULL, unique key (i))")

    tidbWrite(List(row3, row4), schema)

    sql(s"select * from $dbtableWithPrefix").show()
  }

  // spark-3.0 throws the following error
  // Cannot write nullable values to non-null column
  ignore("auto increment column insert null by sql") {
    jdbcUpdate(s"drop table if exists t")
    jdbcUpdate(s"create table t(a int auto_increment primary key)")

    spark.sql("drop table if exists default.st1")
    spark.sql(s"""
                 |CREATE TABLE default.st1
                 |USING tidb
                 |OPTIONS (
                 |  database '$database',
                 |  table 't',
                 |  tidb.addr '$tidbAddr',
                 |  tidb.password '$tidbPassword',
                 |  tidb.port '$tidbPort',
                 |  tidb.user '$tidbUser',
                 |  spark.tispark.pd.addresses '$pdAddresses'
                 |)
       """.stripMargin)

    spark.sql("insert into default.st1 values(null)")

    sql(s"select * from $databaseWithPrefix.t").show()

    assert(queryTiDBViaJDBC(s"select count(*) from $database.t").head.head.toString.equals("1"))
  }
}
