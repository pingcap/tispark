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

import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.TiSessionCatalog
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.test.SharedSQLContext

import scala.collection.mutable.ArrayBuffer

class BaseTiSparkTest extends QueryTest with SharedSQLContext {

  protected var tidbStmt: Statement = _

  private val defaultTestDatabases: Seq[String] = Seq("tispark_test")

  protected var tableNames: Seq[String] = _

  private def tiCatalog = ti.tiCatalog

  protected def querySpark(query: String): List[List[Any]] = {
    val df = sql(query)
    val schema = df.schema.fields

    dfData(df, schema)
  }

  protected def queryTiDB(query: String): List[List[Any]] = {
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

  protected def createOrReplaceTempView(dbName: String,
                                        viewName: String,
                                        postfix: String = "_j"): Unit =
    spark.read
      .format("jdbc")
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_TABLE_NAME, s"`$dbName`.`$viewName`")
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "com.mysql.jdbc.Driver")
      .load()
      .createOrReplaceTempView(s"`$viewName$postfix`")

  protected def setCurrentDatabase(dbName: String): Unit =
    if (tiCatalog
          .catalogOf(Some(dbPrefix + dbName))
          .exists(_.isInstanceOf[TiSessionCatalog])) {
      tidbConn.setCatalog(dbName)
      initializeTimeZone()
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

  protected def loadTestData(databases: Seq[String] = defaultTestDatabases): Unit =
    try {
      tableNames = Seq.empty[String]
      for (dbName <- databases) {
        setCurrentDatabase(dbName)
        val tableDF = spark.read
          .format("jdbc")
          .option(JDBCOptions.JDBC_URL, jdbcUrl)
          .option(JDBCOptions.JDBC_TABLE_NAME, "information_schema.tables")
          .option(JDBCOptions.JDBC_DRIVER_CLASS, "com.mysql.jdbc.Driver")
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

  protected def loadTestData(testTables: TestTables): Unit = {
    val dbName = testTables.dbName
    setCurrentDatabase(dbName)
    for (tableName <- testTables.tables) {
      createOrReplaceTempView(dbName, tableName)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setLogLevel("WARN")
    loadTestData()
  }

  protected def initializeTimeZone(): Unit = {
    tidbStmt = tidbConn.createStatement()
    // Set default time zone to GMT-7
    tidbStmt.execute(s"SET time_zone = '$timeZoneOffset'")
  }

  protected case class TestTables(dbName: String, tables: String*)

  protected def refreshConnections(testTables: TestTables, isHiveEnabled: Boolean = false): Unit = {
    super.refreshConnections(isHiveEnabled)
    loadTestData(testTables)
  }

  override protected def refreshConnections(isHiveEnabled: Boolean): Unit = {
    super.refreshConnections(isHiveEnabled)
    loadTestData()
  }

  override protected def refreshConnections(): Unit = {
    super.refreshConnections()
    loadTestData()
  }

  def setLogLevel(level: String): Unit =
    spark.sparkContext.setLogLevel(level)

  /** Rename JDBC tables
   *   - currently we use table names with `_j` suffix for JDBC tests
   *
   * @param qSpark spark sql query
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

  protected def judge(str: String, skipped: Boolean = false, checkLimit: Boolean = true): Unit =
    runTest(str, skipped = skipped, skipJDBC = true, checkLimit = checkLimit)

  private def compSparkWithTiDB(sql: String, checkLimit: Boolean = true): Boolean =
    compSqlResult(sql, querySpark(sql), queryTiDB(sql), checkLimit)

  protected def checkSparkResult(sql: String,
                                 result: List[List[Any]],
                                 checkLimit: Boolean = true): Unit =
    assert(compSqlResult(sql, querySpark(sql), result, checkLimit))

  protected def checkSparkResultContains(sql: String,
                                         result: List[Any],
                                         checkLimit: Boolean = true): Unit =
    assert(querySpark(sql).exists(x => compSqlResult(sql, List(x), List(result), checkLimit)))

  protected def explainSpark(str: String, skipped: Boolean = false): Unit =
    try {
      if (skipped) {
        logger.warn(s"Test is skipped. [With Spark SQL: $str]")
      } else {
        spark.sql(str).explain()
      }
    } catch {
      case e: Throwable => fail(e)
    }

  protected def explainAndTest(str: String, skipped: Boolean = false): Unit =
    try {
      explainSpark(str)
      judge(str, skipped)
    } catch {
      case e: Throwable => fail(e)
    }

  protected def explainAndRunTest(qSpark: String,
                                  qJDBC: String = null,
                                  skipped: Boolean = false,
                                  rSpark: List[List[Any]] = null,
                                  rJDBC: List[List[Any]] = null,
                                  rTiDB: List[List[Any]] = null,
                                  skipJDBC: Boolean = false,
                                  skipTiDB: Boolean = false,
                                  checkLimit: Boolean = true): Unit =
    try {
      explainSpark(qSpark)
      if (qJDBC == null) {
        runTest(qSpark, skipped, rSpark, rJDBC, rTiDB, skipJDBC, skipTiDB, checkLimit)
      } else {
        runTestWithoutReplaceTableName(
          qSpark,
          qJDBC,
          skipped,
          rSpark,
          rJDBC,
          rTiDB,
          skipJDBC,
          skipTiDB,
          checkLimit
        )
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
   * @param qSpark      query for TiSpark and TiDB
   * @param rSpark      pre-calculated TiSpark result
   * @param rJDBC       pre-calculated Spark-JDBC result
   * @param rTiDB       pre-calculated TiDB result
   * @param skipJDBC    whether not to run test for Spark-JDBC
   * @param skipTiDB    whether not to run test for TiDB
   * @param checkLimit  whether check if sql contains limit but not order by
   */
  protected def runTest(qSpark: String,
                        skipped: Boolean = false,
                        rSpark: List[List[Any]] = null,
                        rJDBC: List[List[Any]] = null,
                        rTiDB: List[List[Any]] = null,
                        skipJDBC: Boolean = false,
                        skipTiDB: Boolean = false,
                        checkLimit: Boolean = true): Unit =
    runTestWithoutReplaceTableName(
      qSpark,
      replaceJDBCTableName(qSpark, skipJDBC),
      skipped,
      rSpark,
      rJDBC,
      rTiDB,
      skipJDBC,
      skipTiDB,
      checkLimit
    )

  /** Run test with sql `qSpark` for TiSpark and TiDB, `qJDBC` for Spark-JDBC. Throw fail exception when
   *    - TiSpark query throws exception
   *    - Both TiDB and Spark-JDBC queries fails to execute
   *    - Both TiDB and Spark-JDBC results differ from TiSpark result
   *
   *  rSpark, rJDBC and rTiDB are used when we want to guarantee a fixed result which might change due to
   *    - Current incorrectness/instability in used version(s)
   *    - Format differences for partial data types
   *
   * @param qSpark      query for TiSpark and TiDB
   * @param qJDBC       query for Spark-JDBC
   * @param rSpark      pre-calculated TiSpark result
   * @param rJDBC       pre-calculated Spark-JDBC result
   * @param rTiDB       pre-calculated TiDB result
   * @param skipJDBC    whether not to run test for Spark-JDBC
   * @param skipTiDB    whether not to run test for TiDB
   * @param checkLimit  whether check if sql contains limit but not order by
   */
  private def runTestWithoutReplaceTableName(qSpark: String,
                                             qJDBC: String,
                                             skipped: Boolean = false,
                                             rSpark: List[List[Any]] = null,
                                             rJDBC: List[List[Any]] = null,
                                             rTiDB: List[List[Any]] = null,
                                             skipJDBC: Boolean = false,
                                             skipTiDB: Boolean = false,
                                             checkLimit: Boolean = true): Unit = {
    if (skipped) {
      logger.warn(s"Test is skipped. [With Spark SQL: $qSpark]")
      return
    }

    var r1: List[List[Any]] = rSpark
    var r2: List[List[Any]] = rJDBC
    var r3: List[List[Any]] = rTiDB

    if (r1 == null) {
      try {
        r1 = querySpark(qSpark)
      } catch {
        case e: Throwable => fail(e)
      }
    }

    if (skipJDBC && skipTiDB) {
      // If JDBC and TiDB tests are both skipped, the correctness of test is not guaranteed.
      // However the result might still be useful when we only want to test if the query fails in TiSpark.
      logger.warn(
        s"Unknown correctness of test result: Skipped in both JDBC and TiDB. [With Spark SQL: $qSpark]"
      )
      return
    }

    if (!skipJDBC && r2 == null) {
      try {
        r2 = querySpark(qJDBC)
      } catch {
        case e: Throwable =>
          logger.warn(s"Spark with JDBC failed when executing:$qJDBC", e) // JDBC failed
      }
    }

    if (skipJDBC || !compSqlResult(qSpark, r1, r2, checkLimit)) {
      if (!skipTiDB && r3 == null) {
        try {
          r3 = queryTiDB(qSpark)
        } catch {
          case e: Throwable => logger.warn(s"TiDB failed when executing:$qSpark", e) // TiDB failed
        }
      }
      if (skipTiDB || !compSqlResult(qSpark, r1, r3, checkLimit)) {
        fail(
          s"""Failed with
             |TiSpark:\t\t${listToString(r1)}
             |Spark With JDBC:${listToString(r2)}
             |TiDB:\t\t\t${listToString(r3)}""".stripMargin
        )
      }
    }
  }

  private def listToString(result: List[List[Any]]): String =
    if (result == null) s"[len: null] = null"
    else if (result.isEmpty) s"[len: 0] = Empty"
    else s"[len: ${result.length}] = ${result.map(mapStringList).mkString(",")}"

  private def mapStringList(result: List[Any]): String =
    if (result == null) "null" else "List(" + result.map(mapString).mkString(",") + ")"

  private def mapString(result: Any): String =
    if (result == null) "null"
    else
      result match {
        case _: Array[Byte] =>
          var str = "["
          for (s <- result.asInstanceOf[Array[Byte]]) {
            str += " " + s.toString
          }
          str += " ]"
          str
        case _ =>
          result.toString
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

}
