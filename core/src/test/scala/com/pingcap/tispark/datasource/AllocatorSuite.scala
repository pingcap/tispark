package com.pingcap.tispark.datasource

import com.pingcap.tikv.IDAllocator
import org.apache.spark.sql.BaseTiSparkSuite

class AllocatorSuite extends BaseTiSparkSuite {
  test("test unsigned allocator") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("""CREATE TABLE `t` (
                       |  `a` int(11) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)
    refreshConnections()
    val dbName = "tidb_tispark_test"
    val tableName = "t"
    val tiDBInfo = ti.tiSession.getCatalog.getDatabase(dbName)
    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbName, tableName)
    // corner case allocate unsigned long's max value.
    val allocator = new IDAllocator(tiDBInfo.getId, ti.tiSession.getCatalog, true, -1L)
    allocator.alloc(tiTableInfo.getId)
    assert(allocator.getEnd - allocator.getStart == -2L)
  }

  test("test signed allocator") {
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("""CREATE TABLE `t` (
                       |  `a` int(11) DEFAULT NULL
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
      """.stripMargin)
    refreshConnections()
    val dbName = "tidb_tispark_test"
    val tableName = "t"
    val tiDBInfo = ti.tiSession.getCatalog.getDatabase(dbName)
    val tiTableInfo =
      ti.tiSession.getCatalog.getTable(dbName, tableName)
    val allocator = new IDAllocator(tiDBInfo.getId, ti.tiSession.getCatalog, false, 1000)
    allocator.alloc(tiTableInfo.getId)
    assert(allocator.getEnd - allocator.getStart == 999)
  }

}
