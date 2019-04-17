package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row

// with TiExtensions
// will load tidb_config.properties to SparkConf
class DataSourceWithExtensionsSuite
    extends BaseDataSourceSuite("test_data_source_with_extensions", true) {
  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  override def beforeAll(): Unit = {
    super.beforeAll()

    jdbcUpdate(s"drop table if exists $dbtableInJDBC")
    jdbcUpdate(s"create table $dbtableInJDBC(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello'), (2, 'TiDB'), (3, 'Spark'), (4, null)"
    )
  }

  test("Test Simple Comparisons") {
    testFilter("s = 'Hello'", Seq(row1))
    testFilter("i > 2", Seq(row3, row4))
    testFilter("i < 3", Seq(row2))
  }

  test("Test >= and <=") {
    testFilter("i >= 2", Seq(row2, row3, row4))
    testFilter("i <= 3", Seq(row2, row3))
  }

  test("Test logical operators") {
    testFilter("i >= 2 AND i <= 3", Seq(row2, row3))
    testFilter("NOT i = 3", Seq(row2, row4))
    testFilter("NOT i = 3 OR i IS NULL", Seq(row1, row2, row4))
    testFilter("i IS NULL OR i > 2 AND s IS NOT NULL", Seq(row1, row3))
  }

  test("Test IN") {
    testFilter("i IN ( 2, 3)", Seq(row2, row3))
  }

  private def testFilter(filter: String, expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sqlContext.read
      .format("tidb")
      .option("database", databaseInSpark)
      .option("table", testTable)
      .options(tidbOptions)
      .load()
      .filter(filter)
      .sort("i")
    checkAnswer(loadedDf, expectedAnswer)
  }

  override def afterAll(): Unit =
    try {
      jdbcUpdate(s"drop table if exists $dbtableInJDBC")
    } finally {
      super.afterAll()
    }
}
