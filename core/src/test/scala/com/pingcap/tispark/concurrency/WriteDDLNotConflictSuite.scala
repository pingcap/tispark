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

class WriteDDLNotConflictSuite extends ConcurrencyTest {
  test("ddl after GetCommitTS: add column") {
    doTest(s"alter table $dbtable ADD Email varchar(255)")
  }

  test("ddl after GetCommitTS: delete column") {
    doTest(s"alter table $dbtable drop column s")
  }

  test("ddl after GetCommitTS: rename column") {
    doTest(s"alter table $dbtable CHANGE s s2 varchar(128)")
  }

  test("ddl after GetCommitTS: change column type") {
    doTest(s"alter table $dbtable CHANGE i i BIGINT")
  }

  private def doTest(ddl: String): Unit = {
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    new Thread(new Runnable {
      override def run(): Unit = {
        Thread.sleep(sleepBeforeQuery)
        jdbcUpdate(ddl)
      }
    }).start()

    val data: RDD[Row] = sc.makeRDD(List(row1, row2, row3))
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option("sleepAfterGetCommitTS", sleepBeforeQuery * 2)
      .option("useTableLock", "false")
      .mode("append")
      .save()

    compareSelect()
  }
}
