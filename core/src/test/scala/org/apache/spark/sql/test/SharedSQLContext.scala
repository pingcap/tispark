package org.apache.spark.sql.test

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession, TiContext}
import Utils._
import TestConstants._

trait SharedSQLContext extends SQLTestUtils {
  protected val sparkConf = new SparkConf()
  private var _spark: SparkSession = _
  private var _ti: TiContext = _
  private var _tidbConf: Properties = _
  private var _tidbConnection: Connection = _
  private var _sparkJDBC: SparkSession = _
  protected var jdbcUrl: String = _

  protected implicit def spark: SparkSession = _spark

  protected implicit def ti: TiContext = _ti

  protected implicit def jdbc: SparkSession = _sparkJDBC

  protected implicit def tidbConn: Connection = _tidbConnection

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected lazy val createSparkSession: SparkSession = {
    new TestSparkSession(sparkConf)
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
  protected def initializeSession(): Unit = {
    if (_spark == null) {
      _spark = createSparkSession
    }
  }

  private def initializeJDBC(): Unit = {
    if (_sparkJDBC == null) {
      _sparkJDBC = createSparkSession
    }
  }

  protected def initializeTiContext(): Unit = {
    if (_spark != null) {
      _ti = new TiContext(_spark)
    }
  }

  private def initializeTiDB(): Unit = {
    if (_tidbConnection == null) {
      val useRawSparkMySql: Boolean = Utils.getFlag(_tidbConf, KeyUseRawSparkMySql)

      val jdbcUsername =
        if (useRawSparkMySql) getOrThrow(_tidbConf, KeyMysqlUser)
        else getOrThrow(_tidbConf, KeyTiDBUser)

      val jdbcHostname =
        if (useRawSparkMySql) getOrThrow(_tidbConf, KeyMysqlAddress)
        else getOrThrow(_tidbConf, KeyTiDBAddress)

      val jdbcPort =
        if (useRawSparkMySql) 0
        else Integer.parseInt(getOrThrow(_tidbConf, KeyTiDBPort))

      val jdbcPassword =
        if (useRawSparkMySql) getOrThrow(_tidbConf, KeyMysqlPassword)
        else ""

      jdbcUrl = s"jdbc:mysql://$jdbcHostname" +
        (if (useRawSparkMySql) "" else s":$jdbcPort") +
        s"/?user=$jdbcUsername&password=$jdbcPassword"

      logger.info("jdbcUrl: " + jdbcUrl)

      _tidbConnection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    }
  }

  private def initializeConf(): Unit = {
    val confStream = Thread
      .currentThread()
      .getContextClassLoader
      .getResourceAsStream("tidb_config.properties")

    val prop = new Properties()
    prop.load(confStream)
    _tidbConf = prop

    sparkConf.set("spark.tispark.pd.addresses", "127.0.0.1:2379")
  }

  /**
   * Make sure the [[TestSparkSession]] is initialized before any tests are run.
   */
  protected override def beforeAll(): Unit = {
    initializeConf()
    initializeSession()
    initializeTiDB()
    initializeJDBC()
    initializeTiContext()

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
   * Stop the underlying resources, if any.
   */
  protected override def afterAll(): Unit = {
    super.afterAll()
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.stop()
      _spark = null
    }

    if (_sparkJDBC != null) {
      _sparkJDBC.sessionState.catalog.reset()
      _sparkJDBC.stop()
      _sparkJDBC = null
    }

    if (_tidbConnection != null) {
      _tidbConnection.close()
    }
  }
}
