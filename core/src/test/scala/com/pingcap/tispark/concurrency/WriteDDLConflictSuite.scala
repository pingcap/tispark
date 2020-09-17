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

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class WriteDDLConflictSuite extends ConcurrencyTest {
  test("write ddl conflict using TableLock") {
    if (!isEnableTableLock) {
      cancel
    }

    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground(Map("useTableLock" -> "true"))

    Thread.sleep(sleepBeforeQuery)

    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"alter table $dbtable ADD Email varchar(255)")
    }
    assert(
      caught.getMessage
        .startsWith("Table 'test_concurrency_write_read' was locked in WRITE LOCAL by server"))
  }

  test("write ddl conflict using SchemaVersionCheck") {
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleepBeforeQuery)
        jdbcUpdate(s"alter table $dbtable ADD Email varchar(255)")
      }
    }).start()

    val caught = intercept[TiBatchWriteException] {
      val data: RDD[Row] = sc.makeRDD(List(row1, row2, row3))
      val df = sqlContext.createDataFrame(data, schema)
      df.write
        .format("tidb")
        .options(tidbOptions)
        .option("database", database)
        .option("table", table)
        .option("sleepAfterPrewriteSecondaryKey", sleepBeforeQuery * 2)
        .option("useTableLock", "false")
        .mode("append")
        .save()
    }

    assert(caught.getMessage.equals("schema has changed during prewrite!"))
  }
}
