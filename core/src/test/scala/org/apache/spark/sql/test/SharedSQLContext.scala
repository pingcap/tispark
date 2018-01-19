package org.apache.spark.sql.test

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession, TiContext}

trait SharedSQLContext extends SQLTestUtils with SharedSparkSession {
  protected val sparkConf = new SparkConf()
  private var _spark: TestSparkSession = _
  private var _ti: TiContext = _
  private var _tidbConf: Properties = _
  private var _tidbConnection: Connection = _

  /**
    * The [[TestSparkSession]] to use for all tests in this suite.
    */
  protected implicit def spark: SparkSession = _spark

  /**
    * The [[TiContext]] to use for all tests in this suite.
    */
  protected implicit def ti: TiContext = _ti

  /**
    * The [[Connection]] to use for all tests in this suite.
    */
  protected implicit def tidbConn: Connection = _tidbConnection

  /**
    * The [[TestSQLContext]] to use for all tests in this suite.
    */
  protected implicit def sqlContext: SQLContext = _spark.sqlContext

  protected def createSparkSession: TestSparkSession = {
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

  protected def initializeTiContext(): Unit = {
    if (_spark != null) {
      _ti = new TiContext(_spark)
    }
  }

  private def initializeTiDB(): Unit = {
    val confStream = Thread.currentThread()
      .getContextClassLoader
      .getResourceAsStream("tidb_config.properties")

    val prop = new Properties()
    prop.load(confStream)
    _tidbConf = prop

    import Utils._
    import TestConstants._

    val useRawSparkMySql: Boolean = Utils.getFlag(prop, KeyUseRawSparkMySql)

    val jdbcUsername =
      if (useRawSparkMySql) getOrThrow(prop, KeyMysqlUser)
      else                  getOrThrow(prop, KeyTiDBUser)

    val jdbcHostname =
      if (useRawSparkMySql) getOrThrow(prop, KeyMysqlAddress)
      else                  getOrThrow(prop, KeyTiDBAddress)

    val jdbcPort =
      if (useRawSparkMySql) 0
      else                  Integer.parseInt(getOrThrow(prop, KeyTiDBPort))

    val jdbcPassword =
      if (useRawSparkMySql) getOrThrow(prop, KeyMysqlPassword)
      else                  ""

    val jdbcUrl = s"jdbc:mysql://$jdbcHostname" +
      (if (useRawSparkMySql) "" else s":$jdbcPort") +
      s"/?user=$jdbcUsername&password=$jdbcPassword"

    logger.info("jdbcUrl: " + jdbcUrl)

    _tidbConnection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
  }

  /**
    * Make sure the [[TestSparkSession]] is initialized before any tests are run.
    */
  protected override def beforeAll(): Unit = {
    initializeSession()
    initializeTiDB()
    initializeTiContext()

    // Ensure we have initialized the context before calling parent code
    super.beforeAll()
  }

  /**
    * Stop the underlying [[org.apache.spark.SparkContext]], if any.
    */
  protected override def afterAll(): Unit = {
    super.afterAll()
    if (_spark != null) {
      _spark.sessionState.catalog.reset()
      _spark.stop()
      _spark = null
    }
  }
}