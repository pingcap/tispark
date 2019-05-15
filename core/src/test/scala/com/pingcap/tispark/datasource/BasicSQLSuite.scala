package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row

import scala.util.Random

// without TiExtensions
// will not load tidb_config.properties to SparkConf
class BasicSQLSuite extends BaseDataSourceSuite("test_datasource_sql") {
  private val row1 = Row(null, "Hello")
  private val row2 = Row(2, "TiDB")
  private val row3 = Row(3, "Spark")
  private val row4 = Row(4, null)

  // calculated var

  override def beforeAll(): Unit = {
    super.beforeAll()

    dropTable()
    jdbcUpdate(s"create table $dbtableInJDBC(i int, s varchar(128))")
    jdbcUpdate(
      s"insert into $dbtableInJDBC values(null, 'Hello'), (2, 'TiDB')"
    )
  }

  test("Test Select") {
    testSelectSQL(Seq(row1, row2))
  }

  test("Test Insert Into") {
    val tmpTable = "testInsert"
    sqlContext.sql(s"""
                      |CREATE TABLE $tmpTable
                      |USING tidb
                      |OPTIONS (
                      |  database '$database',
                      |  table '$testTable',
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

    testSelectSQL(Seq(row1, row2, row3, row4))
  }

  test("Test Insert Overwrite") {
    val tmpTable = "testOverwrite"
    sqlContext.sql(s"""
                      |CREATE TABLE $tmpTable
                      |USING tidb
                      |OPTIONS (
                      |  database '$database',
                      |  table '$testTable',
                      |  tidb.addr '$tidbAddr',
                      |  tidb.password '$tidbPassword',
                      |  tidb.port '$tidbPort',
                      |  tidb.user '$tidbUser',
                      |  spark.tispark.pd.addresses '$pdAddresses'
                      |)
       """.stripMargin)

    val caught = intercept[TiBatchWriteException] {
      sqlContext.sql(s"""
                        |insert overwrite table $tmpTable values (3, 'Spark'), (4, null)
      """.stripMargin)
    }

    assert(
      caught.getMessage
        .equals("SaveMode: Overwrite is not supported. TiSpark only support SaveMode.Append.")
    )
  }

  private def testSelectSQL(expectedAnswer: Seq[Row]): Unit = {
    val tmpName = s"testSelect_${Math.abs(Random.nextLong())}_${System.currentTimeMillis()}"
    sqlContext.sql(s"""
                      |CREATE TABLE $tmpName
                      |USING tidb
                      |OPTIONS (
                      |  database '$database',
                      |  table '$testTable',
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
      dropTable()
    } finally {
      super.afterAll()
    }
}
