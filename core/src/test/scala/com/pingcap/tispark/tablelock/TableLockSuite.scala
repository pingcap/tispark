package com.pingcap.tispark.tablelock

import com.pingcap.tikv.TiDBJDBCClient
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.datasource.BaseDataSourceTest

class TableLockSuite extends BaseDataSourceTest {

  private var tiDBJDBCClient: TiDBJDBCClient = _

  val queryTemplate = "create table `%s`.`%s`(i INT)"

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

    val table = "lock_and_unlock_tbl"
    // init
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    dropTable(table)
    createTable(queryTemplate, table)

    // lock table
    assert(tiDBJDBCClient.lockTableWriteLocal(database, table))

    // unlock tables
    assert(tiDBJDBCClient.unlockTables())

    // drop table
    dropTable(table)

    // close
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  test("Test TiDBJDBCClient Lock and Write Conflict") {
    if (!isEnableTableLock) {
      cancel
    }

    val table = "lock_tbl_write_conflict"
    // init
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    dropTable(table)
    createTable(queryTemplate, table)

    // lock table
    assert(tiDBJDBCClient.lockTableWriteLocal(database, table))

    // fail write in another jdbc session
    val caught = intercept[java.sql.SQLException] {
      tidbStmt.execute(
        s"insert into `%s`.`%s` values(1),(2),(3),(4),(null)"
      )
    }
    assert(
      caught.getMessage.startsWith(s"Table '$table' was locked in WRITE LOCAL by server")
    )

    // unlock tables
    assert(tiDBJDBCClient.unlockTables())

    // insert data
    tidbStmt.execute(
      s"insert into `%s`.`%s` values(1),(2),(3),(4),(null)"
    )

    // drop table
    dropTable(table)

    // close
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  test("Test TiDBJDBCClient Lock and Read Parallel") {
    if (!isEnableTableLock) {
      cancel
    }

    val table = "lock_tbl_parallel_read"
    // init
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    tiDBJDBCClient = new TiDBJDBCClient(conn)
    dropTable(table)
    createTable(queryTemplate, table)

    // lock table
    assert(tiDBJDBCClient.lockTableWriteLocal(database, table))

    // read data
    tidbStmt.executeQuery(s"select * from `%s`.`%s`")

    // unlock tables
    assert(tiDBJDBCClient.unlockTables())

    // drop table
    dropTable(table)

    // close
    tiDBJDBCClient.close()
    assert(conn.isClosed)
  }

  override def afterAll(): Unit = {
    try {
      if (!tiDBJDBCClient.isClosed) {
        tiDBJDBCClient.unlockTables()
      }
    } catch {
      case _: Throwable =>
    }
  }
}
