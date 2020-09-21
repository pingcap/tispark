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

class AddingIndexReplaceSuite extends BaseBatchWriteTest("adding_index_replace") {
  private val row1 = Row(1, 1, 1, "Hello")
  private val row2 = Row(2, 2, 2, "TiDB")
  private val row3 = Row(3, 3, 3, "Spark")
  private val row4 = Row(4, 4, 4, "abde")
  private val row5 = Row(5, 5, 5, "Duplicate")

  private val conflcitWithOneIndex = Row(6, 4, 6, "abced")
  private val conflcitWithTwoIndices = Row(6, 5, 1, "adedefedede")

  private val schema = StructType(
    List(
      StructField("pk", IntegerType),
      StructField("c1", IntegerType),
      StructField("c2", IntegerType),
      StructField("s", StringType)))

  test("test unique index replace with primary key is handle and index") {
    jdbcUpdate(
      s"create table $dbtable(pk int, c1 int, c2 int, s varchar(128), primary key(pk), unique index(c1), unique index(c2), index(s))")
    jdbcUpdate(s"insert into $dbtable values(1, 1, 1, 'Hello')")
    // insert row2 row3
    tidbWrite(List(row2, row3, row4), schema)
    testTiDBSelect(Seq(row1, row2, row3, row4), "c1")

    val options = Some(Map("replace" -> "true"))

    tidbWrite(List(row5, row5, conflcitWithOneIndex), schema, options)
    testTiDBSelect(Seq(row1, row2, row3, conflcitWithOneIndex, row5), "c1")

    tidbWrite(List(conflcitWithTwoIndices), schema, options)
    testTiDBSelect(Seq(row2, row3, conflcitWithTwoIndices), "c1")
  }

  test("test same key in one rdd") {
    jdbcUpdate(s"create table $dbtable(pk int, c1 int, c2 int, s varchar(128), primary key(pk))")
    jdbcUpdate(s"insert into $dbtable values(1, 1, 1, 'Hello')")

    // insert row2 row3
    tidbWrite(List(row2, row3, row4), schema)
    testTiDBSelect(Seq(row1, row2, row3, row4), "c1")

    val options = Some(Map("replace" -> "true"))
    tidbWrite(List(row2), schema, options)
    testTiDBSelect(Seq(row1, row2, row3, row4), "c1")
  }

  test("test unique index replace with primary key is handle") {
    jdbcUpdate(
      s"create table $dbtable(pk int, c1 int, c2 int, s varchar(128), primary key(pk), unique index(c1), unique index(c2))")
    jdbcUpdate(s"insert into $dbtable values(1, 1, 1, 'Hello')")
    // insert row2 row3
    tidbWrite(List(row2, row3, row4), schema)
    testTiDBSelect(Seq(row1, row2, row3, row4), "c1")

    val options = Some(Map("replace" -> "true"))

    tidbWrite(List(row5, row5, conflcitWithOneIndex), schema, options)
    testTiDBSelect(Seq(row1, row2, row3, conflcitWithOneIndex, row5), "c1")

    tidbWrite(List(conflcitWithTwoIndices), schema, options)
    testTiDBSelect(Seq(row2, row3, conflcitWithTwoIndices), "c1")
  }

  test("test unique index replace without primary key") {
    jdbcUpdate(
      s"create table $dbtable(pk int, c1 int, c2 int, s varchar(128), unique index(c1), unique index(c2))")
    jdbcUpdate(s"insert into $dbtable values(1, 1, 1, 'Hello')")
    // insert row2 row3
    tidbWrite(List(row2, row4, row5), schema)
    testTiDBSelect(Seq(row1, row2, row4, row5), "c1")

    val options = Some(Map("replace" -> "true"))
    tidbWrite(List(row3, row3, conflcitWithOneIndex, conflcitWithTwoIndices), schema, options)
    testTiDBSelect(Seq(row2, row3, conflcitWithOneIndex, conflcitWithTwoIndices), "c1")
  }

  test("test pk is handle") {
    jdbcUpdate(s"create table $dbtable(pk int, c1 int, c2 int, s varchar(128), primary key(pk))")
    jdbcUpdate(s"insert into $dbtable values(1, 1, 1, 'Hello')")
    // insert row2 row3
    tidbWrite(List(row2, row3, row4), schema)
    testTiDBSelect(Seq(row1, row2, row3, row4), "c1")
  }

}
