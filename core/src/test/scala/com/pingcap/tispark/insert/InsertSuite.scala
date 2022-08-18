/*
 * Copyright 2022 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pingcap.tispark.insert

import com.pingcap.tispark.datasource.BaseBatchWriteTest

class InsertSuite extends BaseBatchWriteTest("test_insert_sql") {
  private val source_dbtable = "tispark_test.source_table"
  override def dropTable(): Unit = {
    jdbcUpdate(s"drop table if exists $dbtable")
    jdbcUpdate(s"drop table if exists $source_dbtable")
  }

  test("insert basic test") {
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"SELECT * FROM $dbtable")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert select test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"INSERT INTO $dbtable SELECT * FROM $source_dbtable")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert select with condition test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    spark.sql(s"INSERT INTO $dbtable SELECT i, s FROM $source_dbtable WHERE i >= 1")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(1 == actual)
  }

  test("insert table test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"INSERT INTO $dbtable table $source_dbtable")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert from test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello'),(1,'world')")

    spark.sql(s"INSERT INTO $dbtable FROM $source_dbtable SELECT *")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert from with condition test") {
    jdbcUpdate(
      s"create table $source_dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */,unique key (s))")

    spark.sql(s"INSERT INTO $source_dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    spark.sql(s"INSERT INTO $dbtable FROM $source_dbtable SELECT i,s WHERE i>=1")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(1 == actual)
  }

  test("insert into partition table test") {
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */) partition by hash(i);")

    spark.sql(s"INSERT INTO $dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert with enableUpdateTableStatistics test") {
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */);")

    spark.conf.set("enableUpdateTableStatistics", "true")
    spark.conf.set("tidb.addr", "127.0.0.1")
    spark.conf.set("tidb.port", "4000")
    spark.conf.set("tidb.user", "root")
    spark.conf.set("tidb.password", "")
    spark.sql(s"INSERT INTO $dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

  test("insert with rowFormatVersion = 1 test") {
    jdbcUpdate(
      s"create table $dbtable(i int, s varchar(255), k varchar(255),PRIMARY KEY (i)/*T![clustered_index] CLUSTERED */);")

    spark.conf.set("rowFormatVersion", "1")
    spark.sql(s"INSERT INTO $dbtable VALUES(0,'hello','tidb'),(1,'world','test')")

    val actual = spark.sql(s"SELECT count(*) FROM $dbtable").head().get(0)
    assert(2 == actual)
  }

}
