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

import java.util.concurrent.atomic.AtomicInteger

class WriteReadConflictWithUniqueKeySuite extends ConcurrentcyTest {
  test("read conflict with unique key using jdbc") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128), UNIQUE KEY(i))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground()

    // query via jdbc
    val resultRowCount = new AtomicInteger(0)
    val readThread1 = newJDBCReadThread(1, resultRowCount)
    val readThread2 = newJDBCReadThread(2, resultRowCount)
    val readThread3 = newJDBCReadThread(3, resultRowCount)

    readThread1.start()
    readThread2.start()
    readThread3.start()

    readThread1.join()
    readThread2.join()
    readThread3.join()

    // Empty Result or Resolve Lock Timeout
    assert(resultRowCount.get() == 0)
  }

  test("read conflict with unique key using tispark") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128), UNIQUE KEY(i))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground()

    // query via jdbc
    val resultRowCount = new AtomicInteger(0)
    val readThread1 = newTiSparkReadThread(1, resultRowCount)
    val readThread2 = newTiSparkReadThread(2, resultRowCount)
    val readThread3 = newTiSparkReadThread(3, resultRowCount)

    readThread1.start()
    readThread2.start()
    readThread3.start()

    readThread1.join()
    readThread2.join()
    readThread3.join()

    // Resolve Lock Timeout
    assert(resultRowCount.get() == 0)
  }
}
