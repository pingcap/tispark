package com.pingcap.tispark.datasource

import com.pingcap.tikv.allocator.RowIDAllocator
import org.apache.spark.sql.BaseTiSparkTest

class RowIDAllocatorSuite extends BaseTiSparkTest {
  test("test unsigned allocator") {
    tidbStmt.execute("drop table if exists rowid_allocator")
    tidbStmt.execute("""CREATE TABLE `rowid_allocator` (
                       |  `a` int(11) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)

    val dbName = "tidb_tispark_test"
    val tableName = "rowid_allocator"
    val tiDBInfo = ti.tiSession.getCatalog.getDatabase(dbName)
    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbName, tableName)
    // corner case allocate unsigned long's max value.
    val allocator =
      RowIDAllocator.create(tiDBInfo.getId, tiTableInfo.getId, ti.tiSession.getConf, true, -2L)
    assert(allocator.getEnd - allocator.getStart == -2L)
  }

  test("test signed allocator") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("""CREATE TABLE `t` (
                       |  `a` int(11) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)

    val dbName = "tidb_tispark_test"
    val tableName = "t"
    val tiDBInfo = ti.tiSession.getCatalog.getDatabase(dbName)
    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbName, tableName)
    var allocator =
      RowIDAllocator.create(tiDBInfo.getId, tiTableInfo.getId, ti.tiSession.getConf, false, 1000)
    assert(allocator.getEnd - allocator.getStart == 1000)

    allocator = RowIDAllocator
      .create(tiDBInfo.getId, tiTableInfo.getId, ti.tiSession.getConf, false, 10000)
    assert(allocator.getEnd - allocator.getStart == 10000)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists t")
      tidbStmt.execute("drop table if exists rowid_allocator")
    } finally {
      super.afterAll()
    }

}
