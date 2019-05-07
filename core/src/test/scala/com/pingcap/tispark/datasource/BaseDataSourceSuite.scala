package com.pingcap.tispark.datasource

import java.sql.Statement

import com.pingcap.tispark.TiConfigConst._
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.test.TestConstants._

// Tow modes:
// 1. without TiExtensions:
// set isTidbConfigPropertiesInjectedToSparkEnabled = true
// will not load tidb_config.properties to SparkConf
// 2. with TiExtensions
// set isTidbConfigPropertiesInjectedToSparkEnabled = false
// will load tidb_config.properties to SparkConf
class BaseDataSourceSuite(val testTable: String,
                          val _enableTidbConfigPropertiesInjectedToSpark: Boolean = false)
    extends QueryTest
    with SharedSQLContext {
  protected val database: String = "tispark_test"
  protected val dbtableInJDBC = s"$database.$testTable"
  protected var databaseInSpark: String = _
  protected var dbtableInSpark: String = _

  protected var tidbStmt: Statement = _

  override def beforeAll(): Unit = {
    enableTidbConfigPropertiesInjectedToSpark = _enableTidbConfigPropertiesInjectedToSpark

    super.beforeAll()

    databaseInSpark = getTestDatabaseNameInSpark(database)
    dbtableInSpark = s"$databaseInSpark.$testTable"
    tidbStmt = tidbConn.createStatement()
  }

  protected def jdbcUpdate(query: String): Unit =
    tidbStmt.executeUpdate(query)

  protected def getTestDatabaseNameInSpark(database: String): String =
    if (_enableTidbConfigPropertiesInjectedToSpark) {
      s"$dbPrefix$database"
    } else {
      database
    }
}
