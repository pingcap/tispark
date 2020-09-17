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

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

class InsertSuite extends BaseBatchWriteTest("test.datasource_insert") {
  private val row1 = Row(null, "Hello")
  private val row5 = Row(5, "Duplicate")

  private val row2_v2 = Row(2, "TiSpark")

  private val schema = StructType(
    List(StructField("i", IntegerType), StructField("s", StringType)))

  private def compareRow(r1: Row, r2: Row): Boolean = {
    r1.getAs[Int](0) < r2.getAs[Int](0)
  }

  test("Test insert to table without primary key") {
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(null, 'Hello')")

    var data = List(row1)
    // insert 2 rows
    var insert = generateData(2, 2)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert duplicate row
    insert = generateData(2, 2)
    data = data ::: insert
    // sort the data
    data = data.sortWith(compareRow)
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key (primary key is handle)") {
    jdbcUpdate(s"create table $dbtable(i int primary key, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(2, 'TiDB')")

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    var insert = generateData(3, 2)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert duplicate row
    insert = generateData(4, 2)
    intercept[TiBatchWriteException] {
      tidbWrite(insert, schema)
    }

    // insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key with tiny int") {
    jdbcUpdate(s"create table $dbtable(i tinyint primary key, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(2, 'TiDB')")

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key with small int") {
    jdbcUpdate(s"create table $dbtable(i smallint primary key, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(2, 'TiDB')")

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key with medium int") {
    jdbcUpdate(s"create table $dbtable(i mediumint primary key, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(2, 'TiDB')")

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    val insert = generateData(3, 100)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  // currently user provided auto increment value is not supported!
  ignore("Test insert to table with primary key (auto increase case 1)") {
    jdbcUpdate(s"create table $dbtable(i int primary key AUTO_INCREMENT, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(2, 'TiDB')")

    var data = List(Row(2, "TiDB"))
    // insert 2 rows
    var insert = generateData(3, 2)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    // when provide auto id column value but say not provide them in options
    // an exception will be thrown.
    // duplicate pk
    intercept[TiBatchWriteException] {
      tidbWrite(List(row2_v2, row5), schema)
    }

    // when not provide auto id but say provide them in options
    // and exception will be thrown.
    // null pk
    intercept[TiBatchWriteException] {
      tidbWrite(List(Row(null, "abc")), schema)
    }

    //insert ~100 rows
    insert = generateData(5, 95)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)

    //insert ~1000 rows
    insert = generateData(101, 900)
    data = data ::: insert
    tidbWrite(insert, schema)
    testTiDBSelect(data)
  }

  test("Test insert to table with primary key (auto increase case 2)") {
    jdbcUpdate(s"create table $dbtable(i int primary key AUTO_INCREMENT, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable(s) values('Hello')")

    val withOutIDSchema = StructType(List(StructField("s", StringType)))

    var data = List(Row("Hello"))

    // insert 2 rows
    var insert = generateData(30000, 2, skipFirstCol = true)
    data = data ::: insert
    tidbWrite(insert, withOutIDSchema)
    testTiDBSelect(data, "i", "s")

    // insert ~100 rows
    insert = generateData(30002, 98, skipFirstCol = true)
    data = data ::: insert
    tidbWrite(insert, withOutIDSchema)
    testTiDBSelect(data, "i", "s")

    // insert ~1000 rows
    insert = generateData(30100, 900, skipFirstCol = true)
    data = data ::: insert
    tidbWrite(insert, withOutIDSchema)
    testTiDBSelect(data, "i", "s")
  }

  private def generateData(start: Int, length: Int, skipFirstCol: Boolean = false): List[Row] = {
    val strings = Array("Hello", "TiDB", "Spark", null, "TiSpark")
    val ret = ArrayBuffer[Row]()
    for (x <- start until start + length) {
      if (skipFirstCol) {
        ret += Row(strings(x % strings.length))
      } else {
        ret += Row(x, strings(x % strings.length))
      }
    }
    ret.toList
  }
}
