package org.apache.spark.sql

class ViewTestSuite extends BaseTiSparkTest {
  private val table = "test_view"

  test("Test View") {
    dropTbl()

    tidbStmt.execute(s"create table $table(qty INT, price INT);")
    tidbStmt.execute(s"INSERT INTO $table VALUES(3, 50);")

    try {
      tidbStmt.execute(s"CREATE VIEW v AS SELECT qty, price, qty*price AS value FROM $table;")
    } catch {
      case _: Exception => cancel
    }

    refreshConnections()

    judge(s"select * from $table")
    intercept[AnalysisException](spark.sql("select * from v"))

    spark.sql("show tables").show(false)
  }

  private def dropTbl() = {
    tidbStmt.execute(s"drop table if exists $table")
    try {
      tidbStmt.execute("drop view if exists v")
    } catch {
      case _: Exception => cancel
    }
  }
  override def afterAll() = {
    dropTbl()
  }
}
