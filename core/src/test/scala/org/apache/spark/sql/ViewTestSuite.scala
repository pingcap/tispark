package org.apache.spark.sql

class ViewTestSuite extends BaseTiSparkTest {
  private val table = "test_view"

  test("Test View") {
    tidbStmt.execute(s"drop table if exists $table")
    tidbStmt.execute("drop view if exists v")

    tidbStmt.execute(s"create table $table(qty INT, price INT);")

    tidbStmt.execute(s"INSERT INTO $table VALUES(3, 50);")
    tidbStmt.execute(s"CREATE VIEW v AS SELECT qty, price, qty*price AS value FROM $table;")

    judge(s"select * from $table")
  }
}