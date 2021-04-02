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
import java.sql.{Connection, Date, Statement}
import java.util.{Locale, Properties, TimeZone}
import com.pingcap.tikv.{StoreVersion, TiDBJDBCClient, Version}
import com.pingcap.tispark.TiDBUtils
import com.pingcap.tispark.statistics.StatisticsManager
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.TiCatalog
import org.apache.spark.sql.test.SharedSQLContext.timeZone
import org.apache.spark.sql.test.TestConstants._
import org.apache.spark.sql.test.Utils._
import org.apache.spark.{SharedSparkContext, SparkConf, SparkFunSuite}
import org.joda.time.DateTimeZone
import org.scalatest.concurrent.Eventually
import org.slf4j.Logger

import scala.collection.mutable.ArrayBuffer

/**
 * This trait manages basic TiSpark, Spark JDBC, TiDB JDBC
 * tidbConn resource and relevant configurations.
 *
 * `tidb_config.properties` must be provided in test resources folder
 */
trait SharedSQLContext
    extends SparkFunSuite
    with Eventually
    with Logging
    with SharedSparkContext {
  type TiRow = com.pingcap.tikv.row.Row

  override protected val logger: Logger = SharedSQLContext.logger
  private val tiContextCache = new TiContextCache
  protected var jdbcUrl: String = _
  protected var _sparkSession: SparkSession = _
  private var _spark: SparkSession = _
  private var _tidbConf: Properties = _
  private var _tidbConnection: Connection = _
  private var _statement: Statement = _

  // get the current TiContext lazily
  protected def ti: TiContext = tiContextCache.get()

  protected def tidbConn: Connection = _tidbConnection

  protected def tidbOptions: Map[String, String] = SharedSQLContext.tidbOptions

  protected def sql: String => DataFrame = spark.sql _

  protected def spark: SparkSession = _spark

  protected def tpchDBName: String = SharedSQLContext.tpchDBName

  protected def tpcdsDBName: String = SharedSQLContext.tpcdsDBName

  protected def runTPCH: Boolean = SharedSQLContext.runTPCH

  protected def runTPCDS: Boolean = SharedSQLContext.runTPCDS

  protected def defaultTimeZone: TimeZone = SharedSQLContext.timeZone

  protected def refreshConnections(): Unit = refreshConnections(isHiveEnabled = false)

  protected def refreshConnections(isHiveEnabled: Boolean): Unit = {
    stop()
    SharedSparkContext.stop()

    _isHiveEnabled = isHiveEnabled
    init(forceNotLoad = true)
    initializeContext()
    initializeSparkSession()
  }

  protected def init(forceNotLoad: Boolean = false): Unit = {
    initializeConf(forceNotLoad)
  }

  protected def loadData: String = SharedSQLContext.loadData

  protected def checkLoadTiFlashWithRetry(
      tableName: String,
      database: Option[String] = None): Boolean = {
    val check =
      database match {
        case Some(db) =>
          queryTiDBViaJDBC(
            s"select TABLE_SCHEMA from INFORMATION_SCHEMA.TABLES where TABLE_SCHEMA = '$db' && TABLE_NAME = '$tableName'")
        case None =>
          queryTiDBViaJDBC(
            s"select TABLE_SCHEMA from INFORMATION_SCHEMA.TABLES where TABLE_NAME = '$tableName'")
      }

    if (check.isEmpty) {
      throw new RuntimeException(
        s"$tableName not found in TiDB after load. Load Data to TiFlash failed.")
    }
    logger.info(s"Table $tableName found present in ${check.head.head}")
    for (_ <- 0 until 60) {
      // check every 5 secs
      Thread.sleep(5000)
      val available = database match {
        case Some(db) =>
          queryTiDBViaJDBC(
            s"select AVAILABLE from INFORMATION_SCHEMA.TIFLASH_REPLICA where TABLE_SCHEMA = '$db' && TABLE_NAME = '$tableName'")
        case None =>
          queryTiDBViaJDBC(
            s"select AVAILABLE from INFORMATION_SCHEMA.TIFLASH_REPLICA where TABLE_NAME = '$tableName'")
      }

      if (available.nonEmpty && available.head.head.toString == "true") {
        return true
      }
    }
    // timed out after 5 minutes
    false
  }

  protected def loadSQLFile(
      directory: String,
      file: String,
      checkTiFlashReplica: Boolean = false): Unit = {
    val fullFileName = s"$directory/$file.sql"
    initializeJDBCUrl()
    try {
      val path = getClass.getResource("/" + fullFileName).getPath
      import scala.io.Source
      val source = Source.fromFile(path)
      val queryString = source.mkString
      source.close()
      _tidbConnection.setCatalog("mysql")
      initializeStatement()
      _statement.execute(queryString)
      // file name is equivalent to table name
      if (checkTiFlashReplica && !checkLoadTiFlashWithRetry(file)) {
        // TiFlash replica not available after timeout, we will exit the test by exception
        throw new RuntimeException(s"TiFlash replica of table $file not available after timeout.")
      }
      logger.info(s"Load $fullFileName successfully.")
    } catch {
      case e: Exception =>
        logger.error(
          s"Load $fullFileName failed. Maybe the file does not exist or has syntax error.")
        throw e
    }
  }

  protected def supportBatchWrite: Boolean = {
    // currently only the following versions support BatchWrite
    // 3.0.x (x >= 14)
    // 3.1.x (x >= 0)
    // >= 4.0.0
    StoreVersion.minTiKVVersion(Version.BATCH_WRITE, ti.tiSession.getPDClient)
  }

  protected def initializeStatement(): Unit = {
    _statement = _tidbConnection.createStatement()
  }

  protected def supportClusteredIndex: Boolean = {
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    val tiDBJDBCClient = new TiDBJDBCClient(conn)
    tiDBJDBCClient.supportClusteredIndex
  }

  private def enableClusteredIndex(): Unit = {
    if (supportClusteredIndex) {
      val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
      val stmt = conn.createStatement()
      stmt.execute("SET GLOBAL tidb_enable_clustered_index = 1")
      stmt.close()
      conn.close()
    }
  }

  private def disableClusteredIndex(): Unit = {
    if (supportClusteredIndex) {
      val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
      val stmt = conn.createStatement()
      stmt.execute("SET GLOBAL tidb_enable_clustered_index = 0")
      stmt.close()
      conn.close()
    }
  }

  protected def timeZoneOffset: String = SharedSQLContext.timeZoneOffset

  /**
   * The [[TestSparkSession]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def tidbUser: String = SharedSQLContext.tidbUser

  protected def tidbPassword: String = SharedSQLContext.tidbPassword

  protected def tidbAddr: String = SharedSQLContext.tidbAddr

  protected def tidbPort: Int = SharedSQLContext.tidbPort

  protected def tidbStmt: Statement = _statement

  protected def dbPrefix: String = SharedSQLContext.dbPrefix

  protected def catalogPluginMode: Boolean = SharedSQLContext.catalogPluginMode

  protected def pdAddresses: String = SharedSQLContext.pdAddresses

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
    synchronized {
      if (_sparkSession == null) {
        _sparkSession = new TestSparkSession(sc).session
      }
      if (_spark == null) {
        _spark = _sparkSession
      }
    }

  protected def generateData: Boolean = SharedSQLContext.generateData

  protected def generateDataSeed: Long = SharedSQLContext.generateDataSeed.get

  protected def enableTiFlashTest: Boolean = SharedSQLContext.enableTiFlashTest

  override protected def beforeAll(): Unit = {
    try {
      init()
      // initialize spark context
      super.beforeAll()
      initializeSparkSession()
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        fail(
          s"Failed to initialize SQLContext:${e.getMessage}, please check your TiDB cluster and Spark configuration",
          e)
    }
  }

  override protected def afterAll(): Unit = {
    try {
      stop()
    } finally {
      super.afterAll()
    }
  }

  /**
   * Stop the underlying resources, if any.
   */
  def stop(): Unit = {
    tiContextCache.clear()

    if (_spark != null) {
      try {
        SparkSession.clearDefaultSession()
        SparkSession.clearActiveSession()
      } catch {
        case e: Throwable => println(e)
      } finally {
        _spark = null
        _sparkSession = null
      }
    }

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

  private def initializeConf(forceNotLoad: Boolean = false): Unit = {
    initializeJDBC(forceNotLoad)
    initializeSpark()
  }

  private def initializeJDBC(forceNotLoad: Boolean = false): Unit = {
    if (_tidbConnection == null) {
      initializeJDBCUrl()

      logger.info(s"load data is mode: $loadData")

      if (shouldLoadData(loadData) && !forceNotLoad) {
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
  }

  private def initializeJDBCUrl(): Unit = {
    // TODO(Zhexuan Yang) for zero datetime issue, we need further investigation.
    //  https://github.com/pingcap/tispark/issues/1238
    jdbcUrl =
      s"jdbc:mysql://address=(protocol=tcp)(host=$tidbAddr)(port=$tidbPort)/?user=$tidbUser&password=$tidbPassword" +
        s"&useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=round&useSSL=false" +
        s"&rewriteBatchedStatements=true&autoReconnect=true&failOverReadOnly=false&maxReconnects=10" +
        s"&allowMultiQueries=true&serverTimezone=${timeZone.getDisplayName}&sessionVariables=time_zone='$timeZoneOffset'"

    disableClusteredIndex()

    _tidbConnection = TiDBUtils.createConnectionFactory(jdbcUrl)()
    initializeStatement()
  }

  private def initStatistics(): Unit = {
    _tidbConnection.setCatalog("tispark_test")
    initializeStatement()
    logger.info("Analyzing table tispark_test.full_data_type_table_idx...")
    _statement.execute("analyze table tispark_test.full_data_type_table_idx")
    logger.info("Analyzing table tispark_test.full_data_type_table...")
    _statement.execute("analyze table tispark_test.full_data_type_table")
    logger.info("Analyzing table finished.")
    logger.info("Analyzing table resolveLock_test.CUSTOMER...")
    _statement.execute("analyze table resolveLock_test.CUSTOMER")
    logger.info("Analyzing table finished.")
  }

  private def shouldLoadData(loadData: String): Boolean = {
    if ("true".equals(loadData)) {
      true
    } else if ("auto".equals(loadData)) {
      val databases = queryTiDBViaJDBC("show databases").map(a => a.head)
      if (databases.contains("tispark_test") && databases.contains("tpch_test") && databases
          .contains("resolveLock_test")) {
        false
      } else {
        true
      }
    } else {
      false
    }
  }

  private def queryTiDBViaJDBC(query: String): List[List[Any]] = {
    val resultSet = tidbStmt.executeQuery(query)
    val rsMetaData = resultSet.getMetaData
    val retSet = ArrayBuffer.empty[List[Any]]
    val retSchema = ArrayBuffer.empty[String]
    for (i <- 1 to rsMetaData.getColumnCount) {
      retSchema += rsMetaData.getColumnTypeName(i)
    }
    while (resultSet.next()) {
      val row = ArrayBuffer.empty[Any]

      for (i <- 1 to rsMetaData.getColumnCount) {
        row += toOutput(resultSet.getObject(i), retSchema(i - 1))
      }
      retSet += row.toList
    }
    retSet.toList
  }

  private def toOutput(value: Any, colType: String): Any =
    value match {
      case _: BigDecimal =>
        value.asInstanceOf[BigDecimal].setScale(2, BigDecimal.RoundingMode.HALF_UP)
      case _: Date if colType.equalsIgnoreCase("YEAR") =>
        value.toString.split("-")(0)
      case default =>
        default
    }

  private def initializeSpark(): Unit =
    synchronized {
      if (_tidbConf == null) {
        _tidbConf = SharedSQLContext.tidbConf

        conf = new SparkConf(false)

        val propertyNames = _tidbConf.propertyNames()
        while (propertyNames.hasMoreElements) {
          val key: String = propertyNames.nextElement().asInstanceOf[String]
          if (key.startsWith("spark.")) {
            val value = _tidbConf.getProperty(key)
            conf.set(key, value)
          }
        }

        conf.set("spark.tispark.write.allow_spark_sql", "true")
        conf.set("spark.tispark.write.without_lock_table", "true")
        conf.set("spark.tispark.tikv.region_split_size_in_mb", "1")

        if (_isHiveEnabled) {
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
      }
      import com.pingcap.tispark.TiConfigConst._
      conf.set(PD_ADDRESSES, pdAddresses)
      conf.set(ENABLE_AUTO_LOAD_STATISTICS, "true")
      conf.set(ALLOW_INDEX_READ, getFlagOrTrue(_tidbConf, ALLOW_INDEX_READ).toString)
      conf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
      conf.set(REQUEST_ISOLATION_LEVEL, SNAPSHOT_ISOLATION_LEVEL)
      conf.set("spark.sql.extensions", "org.apache.spark.sql.TiExtensions")
      conf.set(DB_PREFIX, dbPrefix)
      if (catalogPluginMode) {
        conf.set("spark.sql.catalog.tidb_catalog", TiCatalog.className)
        conf.set("spark.sql.catalog.tidb_catalog.pd.addresses", pdAddresses)
      }
    }

  private class TiContextCache {
    private var _ti: TiContext = _

    private[test] def clear(): Unit = {
      if (_ti == null) {
        get()
      }

      if (_ti != null) {
        _ti.tiSession.close()
        _ti = null
      }
    }

    private[test] def get(): TiContext = {
      if (_ti == null) {
        assert(_spark != null, "Spark Session should be initialized")
        _ti = TiExtensions.getTiContext(_spark).get
      }
      _ti
    }
  }
}

object SharedSQLContext extends Logging {
  protected val timeZoneOffset = "-7:00"
  protected val logger: Logger = log
  protected val tidbConf: Properties = initializeTiDBConf()
  // Timezone is fixed to a random GMT-7 for those timezone sensitive tests (timestamp_*, date_*, etc)
  protected val timeZone: TimeZone = TimeZone.getTimeZone("GMT-7")
  // JDK time zone
  TimeZone.setDefault(timeZone)
  // Joda time zone
  DateTimeZone.setDefault(DateTimeZone.forTimeZone(timeZone))
  // Add Locale setting
  Locale.setDefault(Locale.CHINA)
  protected val generateData: Boolean =
    getOrElse(tidbConf, SHOULD_GENERATE_DATA, "true").toLowerCase.toBoolean
  protected val generateDataSeed: Option[Long] = {
    var tmpSeed = getOrElse(tidbConf, GENERATE_DATA_SEED, "1234").toLong
    if (tmpSeed == 0) {
      tmpSeed = System.currentTimeMillis()
    }
    if (generateData) {
      logger.info(s"generate data is enabled and seed is $tmpSeed")
    }
    Some(tmpSeed)
  }
  protected val enableTiFlashTest: Boolean =
    getOrElse(tidbConf, ENABLE_TIFLASH_TEST, "false").toBoolean
  protected var tidbUser: String = _
  protected var tidbPassword: String = _
  protected var tidbAddr: String = _
  protected var tidbPort: Int = _
  protected var pdAddresses: String = _
  protected var tidb_catalog: String = _
  protected var tidbOptions: Map[String, String] = _
  protected var loadData: String = _
  protected var tpchDBName: String = _
  protected var tpcdsDBName: String = _
  protected var runTPCH: Boolean = true
  protected var runTPCDS: Boolean = false
  protected var dbPrefix: String = _
  protected var catalogPluginMode: Boolean = _

  readConf()

  def initializeTiDBConf(): Properties = {
    val confStream = Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("tidb_config.properties")

    val prop = new Properties()
    if (confStream != null) {
      prop.load(confStream)
    }

    prop
  }

  def readConf(): Unit = {
    tidbUser = getOrElse(tidbConf, TiDB_USER, "root")

    tidbPassword = getOrElse(tidbConf, TiDB_PASSWORD, "")

    tidbAddr = getOrElse(tidbConf, TiDB_ADDRESS, "127.0.0.1")

    tidbPort = Integer.parseInt(getOrElse(tidbConf, TiDB_PORT, "4000"))

    import com.pingcap.tispark.TiConfigConst._

    pdAddresses = getOrElse(tidbConf, PD_ADDRESSES, "127.0.0.1:2379")

    catalogPluginMode = !"".equals(getOrElse(tidbConf, "spark.sql.catalog.tidb_catalog", ""))

    dbPrefix = if (catalogPluginMode) "" else getOrElse(tidbConf, DB_PREFIX, "tidb_")

    // run TPC-H tests by default and disable TPC-DS tests by default
    tpchDBName = getOrElse(tidbConf, TPCH_DB_NAME, "tpch_test")
    tpcdsDBName = getOrElse(tidbConf, TPCDS_DB_NAME, "")

    loadData = getOrElse(tidbConf, SHOULD_LOAD_DATA, "auto").toLowerCase

    runTPCH = tpchDBName != ""
    runTPCDS = tpcdsDBName != ""

    tidbOptions = Map(
      TiDB_ADDRESS -> tidbAddr,
      TiDB_PASSWORD -> tidbPassword,
      TiDB_PORT -> s"$tidbPort",
      TiDB_USER -> tidbUser,
      PD_ADDRESSES -> pdAddresses,
      "isTest" -> "true")
  }
}
