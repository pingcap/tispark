/*
 * Copyright 2018 PingCAP, Inc.
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

package org.apache.spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class TimezoneTestSuite extends BaseTiSparkTest {
  private val database = "tispark_test"
  private val table = "test_timezone"

  test("Test JDBC Timezone GLOBAL=-7:00 SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = '${this.timeZoneOffset}'")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    tidbStmt.execute(s"insert into $table values ('$testData')")

    val resultData = queryTiDBViaJDBC(s"select * from $table").head.head

    assert(testData.equals(resultData.toString))
  }

  test("Test JDBC Timezone GLOBAL=UTC SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = UTC")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    tidbStmt.execute(s"insert into $table values ('$testData')")

    val resultData = queryTiDBViaJDBC(s"select * from $table").head.head

    assert(testData.equals(resultData.toString))
  }

  // ignored because spark jdbc write do not use local time zone
  ignore("Test Spark JDBC Write Timezone GLOBAL=-7:00 SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = '${this.timeZoneOffset}'")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    val rowData = Row(testData)
    val schema = StructType(List(StructField("c1", StringType)))
    jdbcWrite(List(rowData), schema)

    val resultData = queryTiDBViaJDBC(s"select * from $table").head.head

    assert(testData.equals(resultData.toString))
  }

  // ignored because spark jdbc write do not use local time zone
  ignore("Test Spark JDBC Write Timezone GLOBAL=UTC SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = UTC")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    val rowData = Row(testData)
    val schema = StructType(List(StructField("c1", StringType)))
    jdbcWrite(List(rowData), schema)

    val resultData = queryTiDBViaJDBC(s"select * from $table").head.head

    assert(testData.equals(resultData.toString))
  }

  test("Test Spark TiDB Read Timezone GLOBAL=-7:00 SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = '${this.timeZoneOffset}'")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    tidbStmt.execute(s"insert into $table values ('$testData')")

    val resultData = queryDatasourceTiDB().head().get(0)

    assert(testData.equals(resultData.toString))
  }

  test("Test Spark TiDB Read Timezone GLOBAL=UTC SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = UTC")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    tidbStmt.execute(s"insert into $table values ('$testData')")

    val resultData = queryDatasourceTiDB().head().get(0)

    assert(testData.equals(resultData.toString))
  }

  test("Test Spark TiDB Write Timezone GLOBAL=-7:00 SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = '${this.timeZoneOffset}'")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    val rowData = Row(testData)
    val schema = StructType(List(StructField("c1", StringType)))
    tidbWrite(List(rowData), schema)

    val resultData = queryTiDBViaJDBC(s"select * from $table").head.head

    assert(testData.equals(resultData.toString))
  }

  test("Test Spark TiDB Write Timezone GLOBAL=UTC SESSION=-7:00") {
    tidbStmt.execute(s"SET GLOBAL time_zone = UTC")

    tidbStmt.execute(s"drop table if exists $table")

    tidbStmt.execute(s"create table $table(c1 TIMESTAMP)")
    refreshConnections()

    val testData = "2019-11-11 11:11:11.0"
    val rowData = Row(testData)
    val schema = StructType(List(StructField("c1", StringType)))
    tidbWrite(List(rowData), schema)

    val resultData = queryTiDBViaJDBC(s"select * from $table").head.head

    assert(testData.equals(resultData.toString))
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute(s"drop table if exists $table")
    } finally {
      super.afterAll()
    }

  protected def tidbWrite(
      rows: List[Row],
      schema: StructType,
      param: Option[Map[String, String]] = None): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions ++ param.getOrElse(Map.empty))
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()
  }

  protected def jdbcWrite(rows: List[Row], schema: StructType): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$database.$table")
      .option("isolationLevel", "REPEATABLE_READ")
      .mode("append")
      .save()
  }

  protected def queryDatasourceTiDB(): DataFrame =
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .load()
}
