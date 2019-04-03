/*
 *
 * Copyright 2017 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.test

import java.sql.{Connection, DriverManager, Statement}
import java.util.{Locale, Properties, TimeZone}

import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.test.TestConstants._
import org.apache.spark.sql.test.Utils._
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.joda.time.DateTimeZone
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually
import org.slf4j.Logger

/**
 * This trait manages basic TiSpark, Spark JDBC, TiDB JDBC
 * connection resource and relevant configurations.
 *
 * `tidb_config.properties` must be provided in test resources folder
 */
trait SharedSQLContext extends SparkFunSuite with Eventually with BeforeAndAfterAll {
  protected def spark: SparkSession = SharedSQLContext.spark

  protected def ti: TiContext = SharedSQLContext.ti

  protected def jdbc: SparkSession = SharedSQLContext.jdbc

  protected def tidbConn: Connection = SharedSQLContext.tidbConn

  protected def sql = spark.sql _

  protected def jdbcUrl: String = SharedSQLContext.jdbcUrl

  protected def tpchDBName: String = SharedSQLContext.tpchDBName

  protected def tpcdsDBName: String = SharedSQLContext.tpcdsDBName

  protected def runTPCH: Boolean = SharedSQLContext.runTPCH

  protected def runTPCDS: Boolean = SharedSQLContext.runTPCDS

  protected def dbPrefix: String = SharedSQLContext.dbPrefix

  protected def timeZoneOffset: String = SharedSQLContext.timeZoneOffset

  protected def initStatistics(): Unit = SharedSQLContext.initStatistics()

  protected def defaultTimeZone: TimeZone = SharedSQLContext.timeZone

  protected def refreshConnections(): Unit = SharedSQLContext.refreshConnections()

  protected def paramConf(): Properties = SharedSQLContext._tidbConf

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = spark.sqlContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    try {
      SharedSQLContext.init()
    } catch {
      case e: Throwable =>
        fail(
          s"Failed to initialize SQLContext:${e.getMessage}, please check your TiDB cluster and Spark configuration",
          e
        )
    }
  }
}

object SharedSQLContext extends Logging {
  private val timeZoneOffset = "-7:00"
  // Timezone is fixed to a random GMT-7 for those timezone sensitive tests (timestamp_*, date_*, etc)
  private val timeZone = TimeZone.getTimeZone("GMT-7")
  // JDK time zone
  TimeZone.setDefault(timeZone)
  // Joda time zone
  DateTimeZone.setDefault(DateTimeZone.forTimeZone(timeZone))
  // Add Locale setting
  Locale.setDefault(Locale.CHINA)

  protected val logger: Logger = log
  protected val sparkConf = new SparkConf()
  private var _spark: SparkSession = _
  private var _ti: TiContext = _
  private var _tidbConf: Properties = _
  private var _tidbConnection: Connection = _
  private var _statement: Statement = _
  private var _sparkJDBC: SparkSession = _
  protected var jdbcUrl: String = _
  protected var tpchDBName: String = _
  protected var tpcdsDBName: String = _
  protected var runTPCH: Boolean = true
  protected var runTPCDS: Boolean = false
  protected var dbPrefix: String = _

  protected implicit def spark: SparkSession = _spark

  protected implicit def ti: TiContext = _ti

  protected implicit def jdbc: SparkSession = _sparkJDBC

  protected implicit def tidbConn: Connection = _tidbConnection

  protected implicit def tidbStmt: Statement = _statement

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected var _sparkSession: SparkSession = _

  def refreshConnections(): Unit = {
    stop()
    init(true)
  }

  /**
   * Initialize the [[TestSparkSession]].  Generally, this is just called from
   * beforeAll; however, in test using styles other than FunSuite, there is
   * often code that relies on the session between test group constructs and
   * the actual tests, which may need this session.  It is purely a semantic
   * difference, but semantically, it makes more sense to call
   * 'initializeSession' between a 'describe' and an 'it' call than it does to
   * call 'beforeAll'.
   */
  protected def initializeSession(): Unit =
    if (_spark == null) {
      _spark = _sparkSession
    }

  private def initializeJDBC(): Unit =
    if (_sparkJDBC == null) {
      _sparkJDBC = _sparkSession
    }

  protected def initializeTiContext(): Unit =
    if (_spark != null && _ti == null) {
      _ti = _spark.sessionState.planner.extraPlanningStrategies.head
        .asInstanceOf[TiStrategy]
        .getOrCreateTiContext(_spark)
    }

  protected def initStatistics(): Unit = {
    _tidbConnection.setCatalog("tispark_test")
    _statement = _tidbConnection.createStatement()
    logger.info("Analyzing table tispark_test.full_data_type_table_idx...")
    _statement.execute("analyze table tispark_test.full_data_type_table_idx")
    logger.info("Analyzing table tispark_test.full_data_type_table...")
    _statement.execute("analyze table tispark_test.full_data_type_table")
    logger.info("Analyzing table finished.")
    logger.info("Analyzing table resolveLock_test.CUSTOMER...")
    _statement.execute("analyze table resolveLock_test.CUSTOMER")
    logger.info("Analyzing table finished.")
  }

  private def initializeTiDB(forceNotLoad: Boolean = false): Unit =
    if (_tidbConnection == null) {
      val jdbcUsername = getOrElse(_tidbConf, TiDB_USER, "root")

      val jdbcPassword = getOrElse(_tidbConf, TiDB_PASSWORD, "")

      val jdbcHostname = getOrElse(_tidbConf, TiDB_ADDRESS, "127.0.0.1")

      val jdbcPort = Integer.parseInt(getOrElse(_tidbConf, TiDB_PORT, "4000"))

      val loadData = getOrElse(_tidbConf, SHOULD_LOAD_DATA, "true").toLowerCase.toBoolean

      jdbcUrl =
        s"jdbc:mysql://address=(protocol=tcp)(host=$jdbcHostname)(port=$jdbcPort)/?user=$jdbcUsername&password=$jdbcPassword&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false&rewriteBatchedStatements=true"

      _tidbConnection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
      _statement = _tidbConnection.createStatement()

      if (loadData) {
        logger.info("load data is enabled")
      } else {
        logger.info("load data is disabled")
      }

      if (loadData && !forceNotLoad) {
        logger.info("Loading TirkTestData")
        // Load index test data
        var queryString = resourceToString(
          s"tispark-test/IndexTest.sql",
          classLoader = Thread.currentThread().getContextClassLoader
        )
        _statement.execute(queryString)
        logger.info("Load IndexTest.sql successfully.")
        // Load expression test data
        queryString = resourceToString(
          s"tispaest/TiSparkTest.sql",
          classLoader = Thread.currentThread().getContextClassLoader
        )
        _statement.execute(queryString)
        logger.info("Load TiSparkTest.sql successfully.")
        // Load tpch test data
        queryString = resourceToString(
          s"tispark-test/TPCHData.sql",
          classLoader = Thread.currentThread().getContextClassLoader
        )
        _statement.execute(queryString)
        logger.info("Load TPCHData.sql successfully.")
        // Load resolveLock test data
        queryString = resourceToString(
          s"resolveLock-test/ddl.sql",
          classLoader = Thread.currentThread().getContextClassLoader
        )
        _statement.execute(queryString)
        logger.info("Load resolveLock-test.ddl.sql successfully.")
        initStatistics()
      }
    }

  private def initializeConf(): Unit =
    if (_tidbConf == null) {
      val confStream = Thread
        .currentThread()
        .getContextClassLoader
        .getResourceAsStream("tidb_config.properties")

      val prop = new Properties()
      if (confStream != null) {
        prop.load(confStream)
      }

      import com.pingcap.tispark.TiConfigConst._
      sparkConf.set(PD_ADDRESSES, getOrElse(prop, PD_ADDRESSES, "127.0.0.1:2379"))
      sparkConf.set(ENABLE_AUTO_LOAD_STATISTICS, "true")
      sparkConf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
      sparkConf.set(REQUEST_ISOLATION_LEVEL, SNAPSHOT_ISOLATION_LEVEL)
      sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")

      dbPrefix = getOrElse(prop, DB_PREFIX, "tidb_")
      sparkConf.set(DB_PREFIX, dbPrefix)

      // run TPC-H tests by default and disable TPC-DS tests by default
      tpchDBName = getOrElse(prop, TPCH_DB_NAME, "tpch_test")
      tpcdsDBName = getOrElse(prop, TPCDS_DB_NAME, "")

      runTPCH = tpchDBName != ""
      runTPCDS = tpcdsDBName != ""

      _tidbConf = prop
      _sparkSession = new TestSparkSession(sparkConf).session
    }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  def init(forceNotLoad: Boolean = false): Unit = {
    initializeConf()
    initializeSession()
    initializeTiDB(forceNotLoad)
    initializeJDBC()
    initializeTiContext()
  }

  /**
   * Stop the underlying resources, if any.
   */
  def stop(): Unit = {
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.stop()
      _spark = null
    }

    if (_ti != null) {
      _ti.sparkSession.sessionState.catalog.reset()
      _ti.sparkSession.stop()
      _ti.tiSession.close()
      _ti = null
    }

    if (_sparkJDBC != null) {
      _sparkJDBC.sessionState.catalog.reset()
      _sparkJDBC.stop()
      _sparkJDBC = null
    }

    if (_tidbConnection != null) {
      _tidbConnection.close()
      _tidbConnection = null
    }

    // Reset statisticsManager in case it use older version of TiContext
    StatisticsManager.reset()

    if (_tidbConf != null) {
      _tidbConf = null
    }
  }
}
