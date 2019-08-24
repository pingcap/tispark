package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row

class FilterPushdownSuite extends BaseDataSourceTest {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)
  val table = "test_datasource_filter_pushdown"

  override def beforeAll(): Unit = {
    super.beforeAll()

    dropTable(table)
    createTable("create table `%s`.`%s`(i int, s varchar(128))", table)
    jdbcUpdate(
      "insert into `%s`.`%s` values(null, 'Hello'), (2, 'TiDB'), (3, 'Spark'), (4, null)",
      table
    )
  }

  test("Test Simple Comparisons") {
    testTiDBSelectFilter(table, "s = 'Hello'", Seq(row1))
    testTiDBSelectFilter(table, "i > 2", Seq(row3, row4))
    testTiDBSelectFilter(table, "i < 3", Seq(row2))
  }

  test("Test >= and <=") {
    testTiDBSelectFilter(table, "i >= 2", Seq(row2, row3, row4))
    testTiDBSelectFilter(table, "i <= 3", Seq(row2, row3))
  }

  test("Test logical operators") {
    testTiDBSelectFilter(table, "i >= 2 AND i <= 3", Seq(row2, row3))
    testTiDBSelectFilter(table, "NOT i = 3", Seq(row2, row4))
    testTiDBSelectFilter(table, "NOT i = 3 OR i IS NULL", Seq(row1, row2, row4))
    testTiDBSelectFilter(table, "i IS NULL OR i > 2 AND s IS NOT NULL", Seq(row1, row3))
  }

  test("Test IN") {
    testTiDBSelectFilter(table, "i IN ( 2, 3)", Seq(row2, row3))
  }
}
