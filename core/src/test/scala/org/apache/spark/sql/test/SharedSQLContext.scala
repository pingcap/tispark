package org.apache.spark.sql.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession, TiContext}

trait SharedSQLContext extends SQLTestUtils with SharedSparkSession {
  protected val sparkConf = new SparkConf()
  private var _spark: TestSparkSession = _
  private var _ti: TiContext = _

  /**
    * The [[TestSparkSession]] to use for all tests in this suite.
    */
  protected implicit def spark: SparkSession = _spark

  /**
    * The [[TiContext]] to use for all tests in this suite.
    */
  protected implicit def ti: TiContext = _ti

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

  /**
    * Make sure the [[TestSparkSession]] is initialized before any tests are run.
    */
  protected override def beforeAll(): Unit = {
    initializeSession()

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