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

package com.pingcap.tispark

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType,
  TimestampType
}

class BatchWriteIssueSuite extends BaseDataSourceTest("test_batchwrite_issue") {
  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  test("Combine unique index with null value test") {
    doTestNullValues(s"create table $dbtable(a int, b varchar(64), CONSTRAINT ab UNIQUE (a, b))")
  }

  test("Combine primary key with null value test") {
    doTestNullValues(s"create table $dbtable(a int, b varchar(64), PRIMARY KEY (a, b))")
  }

  test("PK is handler with null value test") {
    doTestNullValues(s"create table $dbtable(a int, b varchar(64), PRIMARY KEY (a))")
  }

  test("Index for timestamp was written multiple times") {
    if (!supportBatchWrite) {
      cancel
    }

    val schema = StructType(
      List(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", TimestampType)))
    val options = Some(Map("replace" -> "true"))

    dropTable()
    jdbcUpdate(
      s"create table $dbtable(a int, b varchar(64), c datetime, CONSTRAINT xx UNIQUE (b), key `dt_index` (c))")

    for (_ <- 0 to 1) {
      val row1 = Row(10, "1", java.sql.Timestamp.valueOf("2001-12-29 22:44:04"))
      val row2 = Row(20, "2", java.sql.Timestamp.valueOf("2001-12-29 23:10:31"))
      val row3 = Row(30, "3", java.sql.Timestamp.valueOf("2001-12-29 23:27:14"))
      val row4 = Row(40, "4", java.sql.Timestamp.valueOf("2001-12-29 23:18:46"))
      val row5 = Row(50, "5", java.sql.Timestamp.valueOf("2001-12-29 23:21:45"))
      tidbWrite(List(row1, row2, row3, row4, row5), schema, options)

      try {
        assert(spark.sql(s"select count(c) from $table").collect().head.get(0) === 5)
        assert(spark.sql(s"select count(a) from $table").collect().head.get(0) === 5)
      } finally {
        spark.sql(s"select count(c) from $table").show(false)
        spark.sql(s"select count(c) from $table").explain
        spark.sql(s"select count(a) from $table").show(false)
        spark.sql(s"select count(a) from $table").explain
      }

    }
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }

  private def doTestNullValues(createTableSQL: String): Unit = {
    if (!supportBatchWrite) {
      cancel
    }
    val schema = StructType(
      List(
        StructField("a", IntegerType),
        StructField("b", StringType),
        StructField("c", StringType)))

    val options = Some(Map("replace" -> "true"))

    dropTable()
    jdbcUpdate(createTableSQL)
    jdbcUpdate(s"alter table $dbtable add column to_delete int")
    jdbcUpdate(s"alter table $dbtable add column c varchar(64) default 'c33'")
    jdbcUpdate(s"alter table $dbtable drop column to_delete")
    jdbcUpdate(s"""
                  |insert into $dbtable values(11, 'c12', null);
                  |insert into $dbtable values(21, 'c22', null);
                  |insert into $dbtable (a, b) values(31, 'c32');
                  |insert into $dbtable values(41, 'c42', 'c43');
                  |
      """.stripMargin)

    assert(queryTiDBViaJDBC(s"select c from $dbtable where a=11").head.head == null)
    assert(queryTiDBViaJDBC(s"select c from $dbtable where a=21").head.head == null)
    assert(
      queryTiDBViaJDBC(s"select c from $dbtable where a=31").head.head.toString.equals("c33"))
    assert(
      queryTiDBViaJDBC(s"select c from $dbtable where a=41").head.head.toString.equals("c43"))

    {
      val row1 = Row(11, "c12", "c13")
      val row3 = Row(31, "c32", null)

      tidbWrite(List(row1, row3), schema, options)

      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=11").head.head.toString.equals("c13"))
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=21").head.head == null)
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=31").head.head == null)
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=41").head.head.toString.equals("c43"))
    }

    {
      val row1 = Row(11, "c12", "c213")
      val row3 = Row(31, "c32", "tt")
      tidbWrite(List(row1, row3), schema, options)
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=11").head.head.toString.equals("c213"))
      assert(queryTiDBViaJDBC(s"select c from $dbtable where a=21").head.head == null)
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=31").head.head.toString.equals("tt"))
      assert(
        queryTiDBViaJDBC(s"select c from $dbtable where a=41").head.head.toString.equals("c43"))
    }
  }
}
