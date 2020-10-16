package org.apache.spark.sql

class InvisibleIndexSuite extends BaseTiSparkTest {

  test("invisible index") {
    if (!supportInvisibleIndex) {
      cancel("current version of TiDB does not support invisible index!")
    }

    tidbStmt.execute("drop table if exists t_invisible_index")
    tidbStmt.execute("create table t_invisible_index(a int, index idx_a(a))")
    val tiTableInfo1 =
      ti.tiSession.getCatalog.getTable(dbPrefix + "tispark_test", "t_invisible_index")
    assert(!tiTableInfo1.getIndices.get(0).isInvisible)

    tidbStmt.execute("alter table t_invisible_index alter index idx_a invisible")
    val tiTableInfo2 =
      ti.tiSession.getCatalog.getTable(dbPrefix + "tispark_test", "t_invisible_index")
    assert(tiTableInfo2.getIndices.get(0).isInvisible)
  }

  private lazy val supportInvisibleIndex: Boolean = {
    var result = true
    tidbStmt.execute("drop table if exists t_invisible_index")
    tidbStmt.execute("create table t_invisible_index(a int, index idx_a(a))")
    try {
      tidbStmt.execute("alter table t_invisible_index alter index idx_a invisible")

      val tiTableInfo =
        ti.tiSession.getCatalog.getTable(dbPrefix + "tispark_test", "t_invisible_index")
      result = tiTableInfo.getIndices.get(0).isInvisible
    } catch {
      case e: Throwable => result = false
    }
    result
  }

  override def afterAll(): Unit = {
    try {
      tidbStmt.execute("drop table if exists t_invisible_index")
    } finally {
      super.afterAll()
    }
  }
}
