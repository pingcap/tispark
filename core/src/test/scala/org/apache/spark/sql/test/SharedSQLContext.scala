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

import java.io.File
import java.sql.{Connection, Statement}
import java.util.{Locale, Properties, TimeZone}

import com.pingcap.tispark.TiConfigConst.PD_ADDRESSES
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.test.TestConstants._
import org.apache.spark.sql.test.Utils._
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
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

  protected def tidbConn: Connection = SharedSQLContext.tidbConn

  protected def tidbOptions: Map[String, String] = SharedSQLContext.tidbOptions

  protected def sql: String => DataFrame = spark.sql _

  protected def jdbcUrl: String = SharedSQLContext.jdbcUrl

  protected def tpchDBName: String = SharedSQLContext.tpchDBName

  protected def tpcdsDBName: String = SharedSQLContext.tpcdsDBName

  protected def runTPCH: Boolean = SharedSQLContext.runTPCH

  protected def runTPCDS: Boolean = SharedSQLContext.runTPCDS

  protected def dbPrefix: String = SharedSQLContext.dbPrefix

  protected def timeZoneOffset: String = SharedSQLContext.timeZoneOffset

  protected def initStatistics(): Unit = SharedSQLContext.initStatistics()

  protected def defaultTimeZone: TimeZone = SharedSQLContext.timeZone

  protected def refreshConnections(): Unit = SharedSQLContext.refreshConnections(false)

  protected def refreshConnections(isHiveEnabled: Boolean): Unit =
    SharedSQLContext.refreshConnections(isHiveEnabled)

  protected def loadSQLFile(directory: String, file: String): Unit =
    SharedSQLContext.loadSQLFile(directory, file)

  protected def stop(): Unit = SharedSQLContext.stop()

  protected def paramConf(): Properties = SharedSQLContext._tidbConf

  protected var enableHive: Boolean = false

  protected var enableTidbConfigPropertiesInjectedToSpark: Boolean = true

  protected def tidbUser: String = SharedSQLContext.tidbUser

  protected def tidbPassword: String = SharedSQLContext.tidbPassword

  protected def tidbAddr: String = SharedSQLContext.tidbAddr

  protected def tidbPort: Int = SharedSQLContext.tidbPort

  protected def pdAddresses: String = SharedSQLContext.pdAddresses

  protected def generateData: Boolean = SharedSQLContext.generateData

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = spark.sqlContext

  protected implicit def sc: SparkContext = spark.sqlContext.sparkContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    try {
      SharedSQLContext.init(
        isHiveEnabled = enableHive,
        isTidbConfigPropertiesInjectedToSparkEnabled = enableTidbConfigPropertiesInjectedToSpark
      )
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        fail(
          s"Failed to initialize SQLContext:${e.getMessage}, please check your TiDB cluster and Spark configuration",
          e
        )
    }
  }

  override protected def afterAll(): Unit = {
    super.afterAll()
    stop()
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
  protected var sparkConf: SparkConf = _
  private var _spark: SparkSession = _
  private var _tidbConf: Properties = _
  private var _tidbConnection: Connection = _
  private var _tidbOptions: Map[String, String] = _
  private var _statement: Statement = _
  protected var jdbcUrl: String = _
  protected var tpchDBName: String = _
  protected var tpcdsDBName: String = _
  protected var runTPCH: Boolean = true
  protected var runTPCDS: Boolean = false
  protected var dbPrefix: String = _
  protected var tidbUser: String = _
  protected var tidbPassword: String = _
  protected var tidbAddr: String = _
  protected var tidbPort: Int = _
  protected var pdAddresses: String = _
  protected var generateData: Boolean = _

  protected implicit def spark: SparkSession = _spark

  private class TiContextCache {
    private var _ti: TiContext = _

    private[test] def get: TiContext = {
      if (_ti == null) {
        _ti = _spark.sessionState.planner.extraPlanningStrategies.head
          .asInstanceOf[TiStrategy]
          .getOrCreateTiContext(_spark)
      }
      _ti
    }

    private[test] def clear(): Unit =
      if (_ti != null) {
        _ti.sparkSession.sessionState.catalog.reset()
        _ti.meta.close()
        _ti.sparkSession.close()
        _ti.tiSession.close()
        _ti = null
      }
  }

  private val tiContextCache = new TiContextCache

  // get the current TiContext lazily
  protected implicit def ti: TiContext = tiContextCache.get

  protected implicit def tidbConn: Connection = _tidbConnection

  protected implicit def tidbOptions: Map[String, String] = _tidbOptions

  protected implicit def tidbStmt: Statement = _statement

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected var _sparkSession: SparkSession = _

  def refreshConnections(isHiveEnabled: Boolean): Unit = {
    stop()
    init(forceNotLoad = true, isHiveEnabled = isHiveEnabled)
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
  protected def initializeSparkSession(): Unit =
    if (_spark == null) {
      _spark = _sparkSession
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

  protected def loadSQLFile(directory: String, file: String): Unit = {
    val fullFileName = s"$directory/$file.sql"
    try {
      val queryString = resourceToString(
        fullFileName,
        classLoader = Thread.currentThread().getContextClassLoader
      )

      _tidbConnection.setCatalog("tispark_test")
      _statement = _tidbConnection.createStatement()
      _statement.execute(queryString)
      logger.info(s"Load $fullFileName successfully.")
    } catch {
      case e: Exception =>
        logger.error(
          s"Load $fullFileName failed. Maybe the file does not exist or has syntax error."
        )
        throw e
    }
  }

  private def initializeJDBCUrl(): Unit = {
    tidbUser = getOrElse(_tidbConf, TiDB_USER, "root")

    tidbPassword = getOrElse(_tidbConf, TiDB_PASSWORD, "")

    tidbAddr = getOrElse(_tidbConf, TiDB_ADDRESS, "127.0.0.1")

    tidbPort = Integer.parseInt(getOrElse(_tidbConf, TiDB_PORT, "4000"))

    _tidbOptions = Map(
      TiDB_ADDRESS -> tidbAddr,
      TiDB_PASSWORD -> tidbPassword,
      TiDB_PORT -> s"$tidbPort",
      TiDB_USER -> tidbUser,
      PD_ADDRESSES -> pdAddresses
    )

    jdbcUrl =
      s"jdbc:mysql://address=(protocol=tcp)(host=$tidbAddr)(port=$tidbPort)/?user=$tidbUser&password=$tidbPassword" +
        s"&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&useSSL=false" +
        s"&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false&maxReconnects=10"

    _tidbConnection = TiDBUtils.createConnectionFactory(jdbcUrl)()
    _statement = _tidbConnection.createStatement()
  }

  private def initializeTiDBConnection(forceNotLoad: Boolean = false): Unit =
    if (_tidbConnection == null) {

      initializeJDBCUrl()

      val loadData = getOrElse(_tidbConf, SHOULD_LOAD_DATA, "true").toLowerCase.toBoolean

      if (loadData) {
        logger.info("load data is enabled")
      } else {
        logger.info("load data is disabled")
      }

      if (loadData && !forceNotLoad) {
        logger.info("Loading TiSparkTestData")
        // Load index test data
        loadSQLFile("tispark-test", "IndexTest")
        // Load expression test data
        loadSQLFile("tispark-test", "TiSparkTest")
        // Load TPC-H test data
        loadSQLFile("tispark-test", "TPCHData")
        // Load resolveLock test data
        loadSQLFile("resolveLock-test", "ddl")
        initStatistics()
      }
    }

  private def initializeSparkConf(
    isHiveEnabled: Boolean = false,
    isTidbConfigPropertiesInjectedToSparkEnabled: Boolean = true
  ): Unit =
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

      pdAddresses = getOrElse(prop, PD_ADDRESSES, "127.0.0.1:2379")
      dbPrefix = getOrElse(prop, DB_PREFIX, "tidb_")

      // run TPC-H tests by default and disable TPC-DS tests by default
      tpchDBName = getOrElse(prop, TPCH_DB_NAME, "tpch_test")
      tpcdsDBName = getOrElse(prop, TPCDS_DB_NAME, "")

      runTPCH = tpchDBName != ""
      runTPCDS = tpcdsDBName != ""

      _tidbConf = prop
      sparkConf = new SparkConf()

      generateData = getOrElse(_tidbConf, SHOULD_GENERATE_DATA, "true").toLowerCase.toBoolean

      if (generateData) {
        logger.info("generate data is enabled")
      }

      if (isTidbConfigPropertiesInjectedToSparkEnabled) {
        sparkConf.set(PD_ADDRESSES, pdAddresses)
        sparkConf.set(ENABLE_AUTO_LOAD_STATISTICS, "true")
        sparkConf.set(ALLOW_INDEX_READ, getFlagOrTrue(prop, ALLOW_INDEX_READ).toString)
        sparkConf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
        sparkConf.set(REQUEST_ISOLATION_LEVEL, SNAPSHOT_ISOLATION_LEVEL)
        sparkConf.set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
        sparkConf.set(DB_PREFIX, dbPrefix)
      }

      sparkConf.set("spark.tispark.write.allow_spark_sql", "true")
      sparkConf.set("spark.tispark.write.without_lock_table", "true")

      if (isHiveEnabled) {
        // delete meta store directory to avoid multiple derby instances SPARK-10872
        import java.io.IOException

        import org.apache.commons.io.FileUtils
        val hiveLocalMetaStorePath = new File("metastore_db")
        try FileUtils.deleteDirectory(hiveLocalMetaStorePath)
        catch {
          case e: IOException =>
            e.printStackTrace()
        }
      }

      _sparkSession = new TestSparkSession(sparkConf, isHiveEnabled).session
    }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  def init(forceNotLoad: Boolean = false,
           isHiveEnabled: Boolean = false,
           isTidbConfigPropertiesInjectedToSparkEnabled: Boolean = true): Unit = {
    initializeSparkConf(isHiveEnabled, isTidbConfigPropertiesInjectedToSparkEnabled)
    initializeSparkSession()
    initializeTiDBConnection(forceNotLoad)
  }

  /**
   * Stop the underlying resources, if any.
   */
  def stop(): Unit = {
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.close()
      _spark = null
    }

    tiContextCache.clear()

    if (_statement != null) {
      try {
        _statement.close()
      } catch {
        case _: Throwable =>
      } finally {
        _statement = null
      }

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
