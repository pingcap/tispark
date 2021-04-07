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

package com.pingcap.tispark.concurrency

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

class WriteWriteConflictSuite extends ConcurrencyTest {
  test("write write conflict") {
    if (blockingRead) {
      cancel
    }

    val schema: StructType = StructType(List(StructField("i", IntegerType)))

    jdbcUpdate(s"create table $dbtable(i int primary key)")

    new Thread(new Runnable {
      override def run(): Unit = {
        logger.info("start doBatchWriteInBackground")
        val data: RDD[Row] =
          sc.makeRDD(List(Row(1001), Row(1002), Row(1003), Row(1004), Row(1005)))
        val df = sqlContext.createDataFrame(data, schema)
        df.write
          .format("tidb")
          .options(tidbOptions)
          .option("database", database)
          .option("table", table)
          .option("sleepAfterPrewritePrimaryKey", "20000")
          .option("replace", "true")
          .mode("append")
          .save()
      }
    }).start()

    Thread.sleep(10000)
    val data: RDD[Row] = sc.makeRDD(List(Row(2001), Row(2002), Row(2003), Row(2004), Row(2005)))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()

    spark.sql(s"select * from $dbtableWithPrefix").show(false)
  }

  test("write write conflict using TableLock & jdbc") {
    if (!isEnableTableLock) {
      cancel
    }

    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground(Map("useTableLock" -> "true"))

    Thread.sleep(sleepBeforeQuery)

    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"insert into $dbtable values(5, 'test')")
    }
    assert(
      caught.getMessage
        .startsWith("Table 'test_concurrency_write_read' was locked in WRITE LOCAL by server"))
  }

  test("write write conflict using TableLock & tispark") {
    if (!isEnableTableLock) {
      cancel
    }

    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground(Map("useTableLock" -> "true"))

    Thread.sleep(sleepBeforeQuery)

    val caught = intercept[java.sql.SQLException] {
      val data: RDD[Row] = sc.makeRDD(List(row5))
      val df = sqlContext.createDataFrame(data, schema)
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("useTableLock", "true")
        .mode("append")
        .save()
    }
    assert(
      caught.getMessage
        .startsWith("Table 'test_concurrency_write_read' was locked in WRITE LOCAL by server"))
  }
}
