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

import java.sql.{Connection, DriverManager}
import java.util.Properties

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.sql.{SQLContext, SparkSession, TiContext}
import Utils._
import TestConstants._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.util.resourceToString
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

  /**
   * The [[TestSQLContext]] to use for all tests in this suite.
   */
  protected implicit def sqlContext: SQLContext = spark.sqlContext

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    SharedSQLContext.init()
    spark.sparkContext.setLogLevel("WARN")
  }
}

object SharedSQLContext extends Logging {
  protected val logger: Logger = log
  protected val sparkConf = new SparkConf()
  private var _spark: SparkSession = _
  private var _ti: TiContext = _
  private var _tidbConf: Properties = _
  private var _tidbConnection: Connection = _
  private var _sparkJDBC: SparkSession = _
  protected var jdbcUrl: String = _
  protected var tpchDBName: String = _

  protected lazy val sql = spark.sql _

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
    if (_spark != null && _ti == null) {
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

      val db = getOrThrow(_tidbConf, KeyTestDB)

      jdbcUrl = s"jdbc:mysql://$jdbcHostname" +
        (if (useRawSparkMySql) "" else s":$jdbcPort") +
        s"/?user=$jdbcUsername&password=$jdbcPassword"

      _tidbConnection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)

      logger.warn("Loading TiSparkTestData")
      // Load index test data
      var queryString = resourceToString(
        s"tispark-test/IndexTest.sql",
        classLoader = Thread.currentThread().getContextClassLoader
      )
      tidbConn.createStatement().execute(queryString)
      logger.warn("Loading IndexTest.sql successfully.")
      // Load expression test data
      queryString = resourceToString(
        s"tispark-test/TiSparkTest.sql",
        classLoader = Thread.currentThread().getContextClassLoader
      )
      tidbConn.createStatement().execute(queryString)
      logger.warn("Loading TiSparkTest.sql successfully.")
    }
  }

  private def initializeConf(): Unit = {
    if (_tidbConf == null) {
      val confStream = Thread
        .currentThread()
        .getContextClassLoader
        .getResourceAsStream("tidb_config.properties")

      val prop = new Properties()
      prop.load(confStream)
      tpchDBName = getOrThrow(prop, KeyTPCHDB)

      import com.pingcap.tispark.TiConfigConst._
      sparkConf.set(PD_ADDRESSES, getOrThrow(prop, PD_ADDRESSES))
      sparkConf.set(ALLOW_INDEX_DOUBLE_READ, getOrElse(prop, ALLOW_INDEX_DOUBLE_READ, "true"))
      _tidbConf = prop
    }
  }

  /**
    * Make sure the [[TestSparkSession]] is initialized before any tests are run.
    */
  def init(): Unit = {
    initializeConf()
    initializeSession()
    initializeTiDB()
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