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
class BaseDataSourceSuite(_isTidbConfigPropertiesInjectedToSparkEnabled: Boolean = false)
    extends QueryTest
    with SharedSQLContext {
  protected var tidbStmt: Statement = _
  protected var tidbOptions: Map[String, String] = _

  override def beforeAll(): Unit = {
    isTidbConfigPropertiesInjectedToSparkEnabled = _isTidbConfigPropertiesInjectedToSparkEnabled

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

  protected def getTestDatabaseNameInSpark(database: String): String =
    if (_isTidbConfigPropertiesInjectedToSparkEnabled) {
      s"$dbPrefix$database"
    } else {
      database
    }
}
