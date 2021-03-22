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

package org.apache.spark.sql

import java.sql.Statement

import com.pingcap.tikv.meta.TiTableInfo
import com.pingcap.tikv.{StoreVersion, TiDBJDBCClient, Version}
import com.pingcap.tispark.{TiConfigConst, TiDBUtils}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.execution.ExplainMode
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.test.SharedSQLContext

import scala.collection.mutable.ArrayBuffer

class BaseTiSparkTest extends QueryTest with SharedSQLContext {

  private val defaultTestDatabases: Seq[String] = Seq("tispark_test")

  protected var tableNames: Seq[String] = _

  def beforeAllWithHiveSupport(): Unit = {
    _isHiveEnabled = true
    beforeAllWithoutLoadData()
    loadTestData()
  }

  override def beforeAll(): Unit = {
    _isHiveEnabled = false
    beforeAllWithoutLoadData()
    loadTestData()
  }

  protected def loadTestData(databases: Seq[String] = defaultTestDatabases): Unit =
    try {
      tableNames = Seq.empty[String]
      for (dbName <- databases) {
        setCurrentDatabase(dbName)
        val tableDF = spark.read
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL, jdbcUrl)
          .option(JDBCOptions.JDBC_TABLE_NAME, "information_schema.tables")
          .option(JDBCOptions.JDBC_DRIVER_CLASS, TiDBUtils.TIDB_DRIVER_CLASS)
          .load()
          .filter(s"table_schema = '$dbName'")
          .select("TABLE_NAME")
        val tables = tableDF.collect().map((row: Row) => row.get(0).toString)
        tables.foreach(createOrReplaceTempView(dbName, _))
        tableNames ++= tables
      }
      logger.info("reload test data complete")
    } catch {
      case e: Exception => logger.warn("reload test data failed", e)
    } finally {
      tableNames = tableNames.sorted.reverse
    }

  protected def createOrReplaceTempView(
      dbName: String,
      viewName: String,
      postfix: String = "_j"): Unit =
    spark.read
      .format("jdbc")
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_TABLE_NAME, s"`$dbName`.`$viewName`")
      .option(JDBCOptions.JDBC_DRIVER_CLASS, TiDBUtils.TIDB_DRIVER_CLASS)
      .load()
      .createOrReplaceTempView(s"`$viewName$postfix`")

  protected def setCurrentDatabase(dbName: String): Unit =
    if (catalogPluginMode) {
      if (dbName != "default") {
        tidbConn.setCatalog(dbName)
        initializeStatement()
        spark.sql(s"use tidb_catalog.`$dbName`")
      } else {
        try {
          spark.sql(s"use spark_catalog.`$dbName`")
          logger.warn(s"using database $dbName which does not belong to TiDB, switch to hive")
        } catch {
          case e: NoSuchDatabaseException => fail(e)
        }
      }
    } else {
      if (tiCatalog
          .catalogOf(Some(dbPrefix + dbName))
          .exists(_.isInstanceOf[TiSessionCatalog])) {
        tidbConn.setCatalog(dbName)
        initializeStatement()
        spark.sql(s"use `$dbPrefix$dbName`")
      } else {
        // should be an existing database in hive/meta_store
        try {
          spark.sql(s"use `$dbName`")
          logger.warn(s"using database $dbName which does not belong to TiDB, switch to hive")
        } catch {
          case e: NoSuchDatabaseException => fail(e)
        }
      }
    }

  private def tiCatalog = ti.tiCatalog

  def beforeAllWithoutLoadData(): Unit = {
    super.beforeAll()
    setLogLevel("WARN")
  }

  def setLogLevel(level: String): Unit =
    spark.sparkContext.setLogLevel(level)

  protected def isEnableAlterPrimaryKey: Boolean = {
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    val tiDBJDBCClient = new TiDBJDBCClient(conn)
    tiDBJDBCClient.isEnableAlterPrimaryKey
  }

  protected def isEnableTableLock: Boolean = {
    val conn = TiDBUtils.createConnectionFactory(jdbcUrl)()
    val tiDBJDBCClient = new TiDBJDBCClient(conn)
    tiDBJDBCClient.isEnableTableLock
  }

  protected def supportTTLUpdate: Boolean = {
    StoreVersion.minTiKVVersion(Version.RESOLVE_LOCK_V3, this.ti.tiSession.getPDClient)
  }

  protected def blockingRead: Boolean = {
    !nonBlockingRead
  }

  protected def nonBlockingRead: Boolean = {
    StoreVersion.minTiKVVersion(Version.RESOLVE_LOCK_V4, this.ti.tiSession.getPDClient)
  }

  protected def getTableInfo(databaseName: String, tableName: String): TiTableInfo = {
    ti.meta.getTable(s"$dbPrefix$databaseName", tableName) match {
      case Some(table) => table
      case None => fail(s"table info $databaseName.$tableName not found")
    }
  }

  protected def getTableColumnNames(tableName: String): List[String] = {
    val rs = tidbConn
      .createStatement()
      .executeQuery("select * from " + tableName + " limit 1")
    val metaData = rs.getMetaData
    var resList = ArrayBuffer.empty[String]
    for (i <- 1 to metaData.getColumnCount) {
      resList += metaData.getColumnName(i)
    }
    resList.toList
  }

  protected def refreshConnections(
      testTables: TestTables,
      isHiveEnabled: Boolean = false): Unit = {
    super.refreshConnections(isHiveEnabled)
    loadTestData(testTables)
  }

  protected def loadTestData(testTables: TestTables): Unit = {
    val dbName = testTables.dbName
    setCurrentDatabase(dbName)
    for (tableName <- testTables.tables) {
      createOrReplaceTempView(dbName, tableName)
    }
  }

  override protected def refreshConnections(isHiveEnabled: Boolean): Unit = {
    super.refreshConnections(isHiveEnabled)
    loadTestData()
  }

  override protected def refreshConnections(): Unit = {
    super.refreshConnections()
    loadTestData()
  }

  protected def compSparkWithTiDB(
      qSpark: String,
      qTiDB: String = null,
      checkLimit: Boolean = true): Unit =
    if (qTiDB == null) {
      compSparkWithTiDB(qSpark, checkLimit)
    } else {
      runTest(qSpark, rTiDB = queryTiDBViaJDBC(qTiDB), skipJDBC = true, checkLimit = checkLimit)
    }

  protected def checkSparkResult(
      sql: String,
      result: List[List[Any]],
      checkLimit: Boolean = true): Unit =
    assert(compSqlResult(sql, queryViaTiSpark(sql), result, checkLimit))

  protected def queryViaTiSpark(query: String): List[List[Any]] = {
    val df = sql(query)
    val schema = df.schema.fields

    dfData(df, schema)
  }

  protected def checkSparkResultContains(
      sql: String,
      result: List[Any],
      checkLimit: Boolean = true): Unit =
    assert(
      queryViaTiSpark(sql).exists(x => compSqlResult(sql, List(x), List(result), checkLimit)))

  protected def explainAndTest(str: String, skipped: Boolean = false): Unit =
    try {
      explainSpark(str)
      judge(str, skipped)
    } catch {
      case e: Throwable => fail(e)
    }

  protected def judge(
      str: String,
      skipped: Boolean = false,
      canTestTiFlash: Boolean = false,
      checkLimit: Boolean = true): Unit =
    runTest(
      str,
      skipped = skipped,
      skipJDBC = true,
      canTestTiFlash = canTestTiFlash,
      checkLimit = checkLimit)

  protected def explainAndRunTest(
      qSpark: String,
      qJDBC: String = null,
      skipped: Boolean = false,
      rSpark: List[List[Any]] = null,
      rJDBC: List[List[Any]] = null,
      rTiDB: List[List[Any]] = null,
      rTiFlash: List[List[Any]] = null,
      skipJDBC: Boolean = false,
      skipTiDB: Boolean = false,
      canTestTiFlash: Boolean = false,
      checkLimit: Boolean = true): Unit =
    try {
      explainSpark(qSpark, canTestTiFlash = canTestTiFlash)
      if (qJDBC == null) {
        runTest(
          qSpark = qSpark,
          skipped = skipped,
          rSpark = rSpark,
          rJDBC = rJDBC,
          rTiDB = rTiDB,
          rTiFlash = rTiFlash,
          skipJDBC = skipJDBC,
          skipTiDB = skipTiDB,
          canTestTiFlash = canTestTiFlash,
          checkLimit = checkLimit)
      } else {
        runTestWithoutReplaceTableName(
          qSpark = qSpark,
          qJDBC = qJDBC,
          skipped = skipped,
          rSpark = rSpark,
          rJDBC = rJDBC,
          rTiDB = rTiDB,
          rTiFlash = rTiFlash,
          skipJDBC = skipJDBC,
          skipTiDB = skipTiDB,
          canTestTiFlash = canTestTiFlash,
          checkLimit = checkLimit)
      }
    } catch {
      case e: Throwable => fail(e)
    }

  /** Run test with sql `qSpark` for TiSpark and TiDB, `qJDBC` for Spark-JDBC. Throw fail exception when
   *    - TiSpark query throws exception
   *    - Both TiDB and Spark-JDBC queries fails to execute
   *    - Both TiDB and Spark-JDBC results differ from TiSpark result
   *
   * For JDBC tests we use different view names to distinguish, so table names in Spark SQL will be
   * renamed in Spark-JDBC SQL
   *
   * rSpark, rJDBC and rTiDB are used when we want to guarantee a fixed result which might change due to
   *    - Current incorrectness/instability in used version(s)
   *    - Format differences for partial data types
   *
   * @param qSpark     query for TiSpark and TiDB
   * @param rSpark     pre-calculated TiSpark result
   * @param rJDBC      pre-calculated Spark-JDBC result
   * @param rTiDB      pre-calculated TiDB result
   * @param skipJDBC   whether not to run test for Spark-JDBC
   * @param skipTiDB   whether not to run test for TiDB
   * @param checkLimit whether check if sql contains limit but not order by
   */
  protected def runTest(
      qSpark: String,
      skipped: Boolean = false,
      rSpark: List[List[Any]] = null,
      rJDBC: List[List[Any]] = null,
      rTiDB: List[List[Any]] = null,
      rTiFlash: List[List[Any]] = null,
      skipJDBC: Boolean = false,
      skipTiDB: Boolean = false,
      canTestTiFlash: Boolean = false,
      checkLimit: Boolean = true): Unit =
    runTestWithoutReplaceTableName(
      qSpark = qSpark,
      qJDBC = replaceJDBCTableName(qSpark, skipJDBC),
      skipped = skipped,
      rSpark = rSpark,
      rJDBC = rJDBC,
      rTiDB = rTiDB,
      rTiFlash = rTiFlash,
      skipJDBC = skipJDBC,
      skipTiDB = skipTiDB,
      canTestTiFlash = canTestTiFlash,
      checkLimit = checkLimit)

  /** Rename JDBC tables
   *   - currently we use table names with `_j` suffix for JDBC tests
   *
   * @param qSpark   spark sql query
   * @param skipJDBC if JDBC tests are skipped, no need to rename
   * @return
   */
  private def replaceJDBCTableName(qSpark: String, skipJDBC: Boolean): String = {
    var qJDBC: String = null
    if (!skipJDBC) {
      qJDBC = qSpark + " "
      for (tableName <- tableNames) {
        // tableNames is guaranteed to be in reverse order, so Seq[t, t2, lt]
        // will never be possible, and the following operation holds correct.
        // e.g., for input Seq[t2, t, lt]
        // e.g., select * from t, t2, lt -> select * from t_j, t2_j, lt_j
        qJDBC = qJDBC.replaceAllLiterally(" " + tableName + " ", " " + tableName + "_j ")
        qJDBC = qJDBC.replaceAllLiterally(" " + tableName + ",", " " + tableName + "_j,")
      }
    }
    qJDBC
  }

  /** Run test with sql `qSpark` for TiSpark and TiDB, `qJDBC` for Spark-JDBC. Throw fail exception when
   *    - TiSpark query throws exception
   *    - Both TiDB and Spark-JDBC queries fails to execute
   *    - Both TiDB and Spark-JDBC results differ from TiSpark result
   *
   * rSpark, rJDBC and rTiDB are used when we want to guarantee a fixed result which might change due to
   *    - Current incorrectness/instability in used version(s)
   *    - Format differences for partial data types
   *
   * @param qSpark     query for TiSpark and TiDB
   * @param qJDBC      query for Spark-JDBC
   * @param rSpark     pre-calculated TiSpark result
   * @param rJDBC      pre-calculated Spark-JDBC result
   * @param rTiDB      pre-calculated TiDB result
   * @param rTiFlash   pre-calculated TiFlash result
   * @param skipJDBC   whether not to run test for Spark-JDBC
   * @param skipTiDB   whether not to run test for TiDB
   * @param checkLimit whether check if sql contains limit but not order by
   */
  private def runTestWithoutReplaceTableName(
      qSpark: String,
      qJDBC: String,
      skipped: Boolean = false,
      rSpark: List[List[Any]] = null,
      rJDBC: List[List[Any]] = null,
      rTiDB: List[List[Any]] = null,
      rTiFlash: List[List[Any]] = null,
      skipJDBC: Boolean = false,
      skipTiDB: Boolean = false,
      canTestTiFlash: Boolean = false,
      checkLimit: Boolean = true): Unit = {
    if (skipped) {
      logger.warn(s"Test is skipped. [With Spark SQL: $qSpark]")
      return
    }

    var r1: List[List[Any]] = rSpark
    var r2: List[List[Any]] = rJDBC
    var r3: List[List[Any]] = rTiDB
    var r4: List[List[Any]] = rTiFlash
    val getSparkPlan: String => String = sql =>
      try {
        val dataFrame = spark.sql(sql)
        import org.apache.spark.sql.execution.command.ExplainCommand
        spark.sessionState
          .executePlan(
            ExplainCommand(dataFrame.queryExecution.logical, ExplainMode.fromString("simple")))
          .executedPlan
          .executeCollect()
          .map(_.getString(0))
          .mkString("\n")
      } catch {
        case _: Throwable => ""
      }
    var sparkPlan: String = "null"

    // When enableTiFlashTest is true, only test TiFlash tests, vice versa.
    val shouldTestTiFlash = canTestTiFlash && enableTiFlashTest
    val shouldTestTiKV = !enableTiFlashTest

    if (shouldTestTiKV) {
      if (r1 == null) {
        try {
          r1 = queryViaTiSpark(qSpark)
          sparkPlan = getSparkPlan(qSpark)
        } catch {
          case e: Throwable =>
            try {
              logger.error(s"TiSpark failed when executing: $qSpark", e)
              logger.warn("failure detected with plan: \n", sparkPlan)
            } finally {
              fail(e)
            }
        }
      }

      if (skipJDBC && skipTiDB) {
        // If JDBC and TiDB tests are both skipped, the correctness of test is not guaranteed.
        // However the result might still be useful when we only want to test if the query fails in TiSpark.
        logger.warn(
          s"Unknown correctness of test result: Skipped in both JDBC and TiDB. [With Spark SQL: $qSpark]")
        return
      }

      if (!skipJDBC && r2 == null) {
        try {
          r2 = queryViaTiSpark(qJDBC)
        } catch {
          case e: Throwable =>
            logger.warn(s"Spark with JDBC failed when executing: $qJDBC", e) // JDBC failed
        }
      }

      if (skipJDBC || !compSqlResult(qSpark, r1, r2, checkLimit)) {
        if (!skipTiDB && r3 == null) {
          try {
            r3 = queryTiDBViaJDBC(qSpark)
          } catch {
            case e: Throwable =>
              logger.warn(s"TiDB failed when executing: $qSpark", e) // TiDB failed
          }
        }

        if (skipTiDB || !compSqlResult(qSpark, r1, r3, checkLimit)) {
          fail(s"""Failed with
               |TiSpark:\t\t${listToString(r1)}
               |Spark With JDBC:${listToString(r2)}
               |TiDB:\t\t\t${listToString(r3)}
               |TiSpark Plan:\n$sparkPlan""".stripMargin)
        }
      }
    } else if (shouldTestTiFlash) {
      if (!skipTiDB) {
        // get result from TiDB
        if (r3 == null) {
          try {
            r3 = queryTiDBViaJDBC(qSpark)
          } catch {
            case e: Throwable =>
              logger.warn(s"TiDB failed when executing: $qSpark", e) // TiDB failed
          }
        }
        // get result from TiFlash
        try {
          val prev = spark.conf.getOption(TiConfigConst.ISOLATION_READ_ENGINES)
          spark.conf
            .set(TiConfigConst.ISOLATION_READ_ENGINES, TiConfigConst.TIFLASH_STORAGE_ENGINE)
          r4 = queryViaTiSpark(qSpark)
          sparkPlan = getSparkPlan(qSpark)
          if (!compSqlResult(qSpark, r3, r4, checkLimit)) {
            fail(s"""Failed with
                 |TiFlash:\t\t${listToString(r4)}
                 |TiDB:\t\t\t${listToString(r3)}
                 |TiFlash Plan:\n$sparkPlan""".stripMargin)
          }
          spark.conf.set(
            TiConfigConst.ISOLATION_READ_ENGINES,
            prev.getOrElse(TiConfigConst.DEFAULT_STORAGE_ENGINES))
        } catch {
          case e: Throwable =>
            logger
              .error(s"TiSpark over TiFlash failed when executing: $qSpark", e) // TiFlash failed
            fail(e)
        }
      }
    }
  }

  protected def queryTiDBViaJDBC(
      query: String,
      retryOnFailure: Int = 3,
      stmt: Statement = tidbStmt): List[List[Any]] = {
    val resultSet = callWithRetry(stmt.executeQuery(query), retryOnFailure)
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

  protected def explainSpark(
      str: String,
      skipped: Boolean = false,
      canTestTiFlash: Boolean = false): Unit =
    try {
      if (skipped || (!canTestTiFlash && enableTiFlashTest)) {
        logger.warn(s"Test is skipped. [With Spark SQL: $str]")
      } else {
        spark.sql(str).explain()
      }
    } catch {
      case e: Throwable => fail(e)
    }

  protected def explainTestAndCollect(sql: String): Unit = {
    val df = spark.sql(sql)
    df.explain
    df.show
    df.collect.foreach(println)
  }

  protected def time[A](f: => A): A = {
    val s = System.currentTimeMillis
    val ret = f
    println(s"time: ${(System.currentTimeMillis() - s) / 1e3}s")
    ret
  }

  private def compSparkWithTiDB(sql: String, checkLimit: Boolean): Unit =
    runTest(sql, skipJDBC = true, checkLimit = checkLimit)

  protected case class TestTables(dbName: String, tables: String*)

  protected lazy val supportExpressionIndex: Boolean = {
    var result = true
    tidbStmt.execute("drop table if exists t")
    tidbStmt.execute("create table t (name varchar(64));")
    try {
      tidbStmt.execute("CREATE INDEX idx ON t ((lower(name)));")
    } catch {
      case e: Throwable => result = false
    } finally {
      tidbStmt.execute("drop table if exists t")
    }
    result
  }
}
