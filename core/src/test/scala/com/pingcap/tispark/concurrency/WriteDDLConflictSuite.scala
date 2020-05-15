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

class WriteDDLConflictSuite extends ConcurrentcyTest {
  test("write ddl conflict using jdbc") {
    if (!supportBatchWrite) {
      cancel
    }

    dropTable()
    jdbcUpdate(s"create table $dbtable(i int, s varchar(128))")
    jdbcUpdate(s"insert into $dbtable values(4, 'null')")

    doBatchWriteInBackground()

    Thread.sleep(sleepBeforeQuery)

    val caught = intercept[java.sql.SQLException] {
      jdbcUpdate(s"alter table $dbtable ADD Email varchar(255)")
    }
    assert(
      caught.getMessage
        .startsWith("Table 'test_concurrency_write_read' was locked in WRITE LOCAL by server")
    )
  }
}
