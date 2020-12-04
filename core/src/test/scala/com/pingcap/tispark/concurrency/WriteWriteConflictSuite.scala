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

class WriteWriteConflictSuite extends ConcurrencyTest {

  test("Concurrent Insert") {
      if(blockingRead) {
        cancel
      }

    jdbcUpdate(s"create table $dbtable(i int primary key)")
    (1 to 100).foreach(i => jdbcUpdate(s"insert into $dbtable values ($i)"))

    spark.sql(s"select count(*) from $dbtableWithPrefix").show(false)


  }

  private def runTiSparkInsert()

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
