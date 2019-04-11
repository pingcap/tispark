package com.pingcap.tispark.datasource

import java.sql.Statement

import com.pingcap.tispark.TiConfigConst._
import com.pingcap.tispark.TiUtils.TIDB_SOURCE_NAME
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.TestConstants._
import org.apache.spark.sql.{QueryTest, Row}

class BaseDataSourceWithExtensionsSuite extends QueryTest with SharedSQLContext {
  protected val testDatabase: String = "tispark_test"
  protected val testTable: String = "test_data_source"
  protected val testDBTable = s"$testDatabase.$testTable"

  protected var tidbStmt: Statement = _
  protected var tidbOptions: Map[String, String] = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    tidbStmt = tidbConn.createStatement()

    tidbOptions = Map(
      TiDB_ADDRESS -> tidbAddr,
      TiDB_PASSWORD -> tidbPassword,
      TiDB_PORT -> s"$tidbPort",
      TiDB_USER -> tidbUser,
      PD_ADDRESSES -> pdAddresses
    )
  }

  protected def jdbcUpdate(query: String): Unit =
    tidbStmt.executeUpdate(query)

  /**
   * Verify that the filter is pushed down by looking at the generated SQL,
   * and check the results are as expected
   */
  protected def testFilter(filter: String,
                           expectedWhere: String,
                           expectedAnswer: Seq[Row]): Unit = {
    val database = getTestDatabaseName(testDatabase)
    val loadedDf = sqlContext.read
      .format(TIDB_SOURCE_NAME)
      .options(tidbOptions)
      .option("dbtable", s"$database.$testTable")
      .load()
      .filter(filter)
      .sort("i")
    checkAnswer(loadedDf, expectedAnswer)
  }

  protected def getTestDatabaseName(database: String): String = s"$dbPrefix$database"
}
