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

package com.pingcap.tispark.tablelock

import com.pingcap.tikv.TiDBJDBCClient
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.datasource.BaseDataSourceTest

class TableLockSuite extends BaseDataSourceTest("test_table_lock") {

  private var tiDBJDBCClient: TiDBJDBCClient = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    if (!supportBatchWrite) {
      cancel
    }
  }

  override def afterAll(): Unit = {
    try {
      if (!tiDBJDBCClient.isClosed) {
        tiDBJDBCClient.unlockTables()
      }
    } catch {
      case _: Throwable =>
    }

    try {
      if (!tiDBJDBCClient.isClosed) {
        dropTable()
      }
    } catch {
      case _: Throwable =>
    }

    try {
      super.dropTable()
    } finally {
      super.afterAll()
    }
  }

  override protected def dropTable(): Unit = {
    tiDBJDBCClient.dropTable(database, table)
  }

  test("Test TiDBJDBCClient Close") {
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  test("Test TiDBJDBCClient Lock and Unlock") {
    if (!isEnableTableLock) {
      cancel
    }

    // init
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    createTable()

    // lock table
    assert(tiDBJDBCClient.lockTableWriteLocal(database, table))

    // unlock tables
    assert(tiDBJDBCClient.unlockTables())

    // drop table
    dropTable()

    // close
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  test("Test TiDBJDBCClient Lock and Write Conflict") {
    if (!isEnableTableLock) {
      cancel
    }

    // init
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    createTable()

    // lock table
    assert(tiDBJDBCClient.lockTableWriteLocal(database, table))

    // fail write in another jdbc session
    val caught = intercept[java.sql.SQLException] {
      tidbStmt.execute(s"insert into $dbtable values(1),(2),(3),(4),(null)")
    }
    assert(caught.getMessage.startsWith(s"Table '$table' was locked in WRITE LOCAL by server"))

    // unlock tables
    assert(tiDBJDBCClient.unlockTables())

    // insert data
    tidbStmt.execute(s"insert into $dbtable values(1),(2),(3),(4),(null)")

    // drop table
    dropTable()

    // close
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  test("Test TiDBJDBCClient Lock and Read Parallel") {
    if (!isEnableTableLock) {
      cancel
    }

    // init
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    createTable()

    // lock table
    assert(tiDBJDBCClient.lockTableWriteLocal(database, table))

    // read data
    tidbStmt.executeQuery(s"select * from $dbtable")

    // unlock tables
    assert(tiDBJDBCClient.unlockTables())

    // drop table
    dropTable()

    // close
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  private def createTable(): Unit =
    jdbcUpdate(s"create table $dbtable(i INT)")
}
