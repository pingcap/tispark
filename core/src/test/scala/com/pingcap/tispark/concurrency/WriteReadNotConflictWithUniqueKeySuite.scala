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

class WriteReadNotConflictWithUniqueKeySuite extends ConcurrentcyTest {
  test("read not conflict with unique key using jdbc") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128), UNIQUE KEY(i))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground()

    // query via jdbc
    val resultRowCount = new AtomicInteger(0)
    val readThread4 = newJDBCReadThread(4, resultRowCount)

    readThread4.start()

    readThread4.join()

    assert(resultRowCount.get() == 1)
  }

  test("read not conflict with unique key using tispark") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128), UNIQUE KEY(i))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground()

    // query via jdbc
    val resultRowCount = new AtomicInteger(0)
    val readThread4 = newTiSparkReadThread(4, resultRowCount)

    readThread4.start()
    readThread4.join()

    assert(resultRowCount.get() == 1)
  }
}
