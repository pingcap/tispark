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

class WriteReadSuite extends ConcurrencyTest {

  test("read conflict using jdbc") {
    doTestJDBC(s"create table $dbtable(i int, s varchar(128))")
  }

  test("read conflict using jdbc: primary key") {
    doTestJDBC(s"create table $dbtable(i int, s varchar(128), PRIMARY KEY(i))")
  }

  test("read conflict using jdbc: unique key") {
    doTestJDBC(s"create table $dbtable(i int, s varchar(128), UNIQUE KEY(i))")
  }

  test("read conflict using tispark") {
    doTestTiSpark(s"create table $dbtable(i int, s varchar(128))")
  }

  test("read conflict using tispark: primary key") {
    doTestTiSpark(s"create table $dbtable(i int, s varchar(128), PRIMARY KEY(i))")
  }

  test("read conflict using tispark: unique key") {
    doTestTiSpark(s"create table $dbtable(i int, s varchar(128), UNIQUE KEY(i))")
  }

  def doTestJDBC(createTable: String) = {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(createTable)
    jdbcUpdate(s"insert into $dbtable values(2, 'v2')")
    jdbcUpdate(s"insert into $dbtable values(3, 'v3')")

    // write row1 & row2
    doBatchWriteInBackground()

    // query via jdbc
    val result1 = ConcurrencyTestResult()
    val result2 = ConcurrencyTestResult()
    val result3 = ConcurrencyTestResult()

    val readThread1 = newJDBCReadThread(1, result1)
    val readThread2 = newJDBCReadThread(2, result2)
    val readThread3 = newJDBCReadThread(3, result3)

    readThread1.start()
    readThread2.start()
    readThread3.start()

    readThread1.join()
    readThread2.join()
    readThread3.join()

    if (blockingRead) {
      // Resolve Lock Timeout
      val errorMsg = "Resolve lock timeout"

      assert(result1.hasError)
      assert(result1.error.getCause.getMessage.equals(errorMsg))

      assert(result2.hasError)
      assert(result2.error.getCause.getMessage.equals(errorMsg))

      assert(result3.hasError)
      assert(result3.error.getCause.getMessage.equals(errorMsg))
    } else {
      // non-blocking read old data
      assert(!result1.hasError)
      assert(result1.isEmpty)

      assert(!result2.hasError)
      assert(!result2.isEmpty)
      assert(result2.obj.equals("v2"))

      assert(!result3.hasError)
      assert(!result3.isEmpty)
      assert(result3.obj.equals("v3"))
    }
  }

  def doTestTiSpark(createTable: String) = {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(createTable)
    jdbcUpdate(s"insert into $dbtable values(2, 'v2')")
    jdbcUpdate(s"insert into $dbtable values(3, 'v3')")

    // write row1 & row2
    doBatchWriteInBackground()

    // query via tispark
    val result1 = ConcurrencyTestResult()
    val result2 = ConcurrencyTestResult()
    val result3 = ConcurrencyTestResult()

    val readThread1 = newTiSparkReadThread(1, result1)
    val readThread2 = newTiSparkReadThread(2, result2)
    val readThread3 = newTiSparkReadThread(3, result3)

    readThread1.start()
    readThread2.start()
    readThread3.start()

    readThread1.join()
    readThread2.join()
    readThread3.join()

    if (blockingRead) {
      // Resolve Lock Timeout
      val errorMsg = "com.pingcap.tikv.exception.KeyException: com.pingcap.tikv.txn.Lock"

      assert(result1.hasError)
      assert(result1.error.getMessage.contains(errorMsg))

      assert(result2.hasError)
      assert(result2.error.getMessage.contains(errorMsg))

      assert(result3.hasError)
      assert(result3.error.getMessage.contains(errorMsg))
    } else {
      // non-blocking read old data
      assert(!result1.hasError)
      assert(result1.isEmpty)

      assert(!result2.hasError)
      assert(!result2.isEmpty)
      assert(result2.obj.equals("v2"))

      assert(!result3.hasError)
      assert(!result3.isEmpty)
      assert(result3.obj.equals("v3"))
    }
  }
}
