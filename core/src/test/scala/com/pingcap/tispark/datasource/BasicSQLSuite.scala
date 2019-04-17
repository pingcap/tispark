package com.pingcap.tispark.datasource

import org.apache.spark.sql.Row

import scala.util.Random

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class BasicSQLSuite extends BaseDataSourceSuite {
  private val testTable: String = "test_data_source_sql"

  // Values used for comparison
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  // calculated var
  private val testDBTableInJDBC = s"$testDatabase.$testTable"
  private var testDBTableInSpark: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    testDBTableInSpark = s"${getTestDatabaseNameInSpark(testDatabase)}.$testTable"

    jdbcUpdate(s"drop table if exists $testDBTableInJDBC")
    jdbcUpdate(s"create table $testDBTableInJDBC(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $testDBTableInJDBC values(null, 'Hello'), (2, 'TiDB')"
    )
  }

  test("Test Select") {
    testSelect(testDBTableInSpark, Seq(row1, row2))
  }

  test("Test Insert Into") {
    val dbtable = testDBTableInSpark
    val tmpTable = "testInsert"
    sqlContext.sql(s"""
                      |CREATE TABLE $tmpTable
                      |USING tidb
                      |OPTIONS (
                      |  dbtable '$dbtable',
                      |  tidb.addr '$tidbAddr',
                      |  tidb.password '$tidbPassword',
                      |  tidb.port '$tidbPort',
                      |  tidb.user '$tidbUser',
                      |  spark.tispark.pd.addresses '$pdAddresses'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |insert into $tmpTable values (3, 'Spark'), (4, null)
      """.stripMargin)

    testSelect(testDBTableInSpark, Seq(row1, row2, row3, row4))
  }

  test("Test Insert Overwrite") {
    val dbtable = testDBTableInSpark
    val tmpTable = "testOverwrite"
    sqlContext.sql(s"""
                      |CREATE TABLE $tmpTable
                      |USING tidb
                      |OPTIONS (
                      |  dbtable '$dbtable',
                      |  tidb.addr '$tidbAddr',
                      |  tidb.password '$tidbPassword',
                      |  tidb.port '$tidbPort',
                      |  tidb.user '$tidbUser',
                      |  spark.tispark.pd.addresses '$pdAddresses'
                      |)
       """.stripMargin)

    sqlContext.sql(s"""
                      |insert overwrite table $tmpTable values (3, 'Spark'), (4, null)
      """.stripMargin)

    testSelect(testDBTableInSpark, Seq(row3, row4))
  }

  private def testSelect(dbtable: String, expectedAnswer: Seq[Row]): Unit = {
    val tmpName = s"testSelect_${Math.abs(Random.nextLong())}_${System.currentTimeMillis()}"
    sqlContext.sql(s"""
                      |CREATE TABLE $tmpName
                      |USING tidb
                      |OPTIONS (
                      |  dbtable '$dbtable',
                      |  tidb.addr '$tidbAddr',
                      |  tidb.password '$tidbPassword',
                      |  tidb.port '$tidbPort',
                      |  tidb.user '$tidbUser',
                      |  spark.tispark.pd.addresses '$pdAddresses'
                      |)
       """.stripMargin)
    val df = sqlContext.sql(s"select * from $tmpName sort by i")
    checkAnswer(df, expectedAnswer)
  }

  override def afterAll(): Unit =
    try {
      jdbcUpdate(s"drop table if exists $testDBTableInJDBC")
    } finally {
      super.afterAll()
    }
}
