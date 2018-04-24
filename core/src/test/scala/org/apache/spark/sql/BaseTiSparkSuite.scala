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

import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.test.SharedSQLContext

import scala.collection.mutable.ArrayBuffer

class BaseTiSparkSuite extends QueryTest with SharedSQLContext {

  protected var tidbStmt: Statement = _

  protected def querySpark(query: String): List[List[Any]] = {
    val df = sql(query)
    val schema = df.schema.fields

    dfData(df, schema)
  }

  def queryTiDB(query: String): List[List[Any]] = {
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

  def getTableColumnNames(tableName: String): List[String] = {
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

  def createOrReplaceTempView(dbName: String, viewName: String, postfix: String = "_j"): Unit =
    spark.read
      .format("jdbc")
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_TABLE_NAME, s"`$dbName`.`$viewName`")
      .option(JDBCOptions.JDBC_DRIVER_CLASS, "com.mysql.jdbc.Driver")
      .load()
      .createOrReplaceTempView(s"`$viewName$postfix`")

  def loadTestData(testTables: TestTables = defaultTestTables): Unit = {
    val dbName = testTables.dbName
    tidbConn.setCatalog(dbName)
    ti.tidbMapDatabase(dbName)
    for (tableName <- testTables.tables) {
      createOrReplaceTempView(dbName, tableName)
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    setLogLevel("WARN")
    loadTestData()
    initializeTimeZone()
  }

  def initializeTimeZone(): Unit = {
    tidbStmt = tidbConn.createStatement()
    // Set default time zone to GMT+8
    tidbStmt.execute(s"SET time_zone = '$timeZoneOffset'")
  }

  case class TestTables(dbName: String, tables: String*)

  private val defaultTestTables: TestTables =
    TestTables(dbName = "tispark_test", "full_data_type_table", "full_data_type_table_idx")

  def refreshConnections(testTables: TestTables): Unit = {
    super.refreshConnections()
    loadTestData(testTables)
    initializeTimeZone()
  }

  override def refreshConnections(): Unit = {
    super.refreshConnections()
    loadTestData()
    initializeTimeZone()
  }

  def setLogLevel(level: String): Unit = {
    spark.sparkContext.setLogLevel(level)
  }

  def replaceJDBCTableName(qSpark: String, skipJDBC: Boolean): String = {
    var qJDBC: String = null
    if (!skipJDBC) {
      if (qSpark.contains("full_data_type_table_idx")) {
        qJDBC = qSpark.replace("full_data_type_table_idx", "full_data_type_table_idx_j")
      } else if (qSpark.contains("full_data_type_table")) {
        qJDBC = qSpark.replace("full_data_type_table", "full_data_type_table_j")
      }
    }
    qJDBC
  }

  def judge(str: String, skipped: Boolean = false): Unit =
    assert(execDBTSAndJudge(str, skipped))

  def execDBTSAndJudge(str: String, skipped: Boolean = false): Boolean =
    try {
      if (skipped) {
        logger.warn(s"Test is skipped. [With Spark SQL: $str]")
        true
      } else {
        compResult(querySpark(str), queryTiDB(str), str.contains(" order by "))
      }
    } catch {
      case e: Throwable => fail(e)
    }

  def explainSpark(str: String, skipped: Boolean = false): Unit =
    try {
      if (skipped) {
        logger.warn(s"Test is skipped. [With Spark SQL: $str]")
      } else {
        spark.sql(str).explain()
      }
    } catch {
      case e: Throwable => fail(e)
    }

  def explainAndTest(str: String, skipped: Boolean = false): Unit =
    try {
      explainSpark(str)
      judge(str, skipped)
    } catch {
      case e: Throwable => fail(e)
    }

  def explainAndRunTest(qSpark: String,
                        qJDBC: String = null,
                        skipped: Boolean = false,
                        rSpark: List[List[Any]] = null,
                        rJDBC: List[List[Any]] = null,
                        rTiDB: List[List[Any]] = null,
                        skipJDBC: Boolean = false,
                        skipTiDB: Boolean = false): Unit = {
    try {
      explainSpark(qSpark)
      if (qJDBC == null) {
        runTestWithReplaceTableName(qSpark, skipped, rSpark, rJDBC, rTiDB, skipJDBC, skipTiDB)
      } else {
        runTest(qSpark, qJDBC, skipped, rSpark, rJDBC, rTiDB, skipJDBC, skipTiDB)
      }
    } catch {
      case e: Throwable => fail(e)
    }
  }

  def runTestWithReplaceTableName(qSpark: String,
                                  skipped: Boolean = false,
                                  rSpark: List[List[Any]] = null,
                                  rJDBC: List[List[Any]] = null,
                                  rTiDB: List[List[Any]] = null,
                                  skipJDBC: Boolean = false,
                                  skipTiDB: Boolean = false): Unit = {
    runTest(
      qSpark,
      replaceJDBCTableName(qSpark, skipJDBC),
      skipped,
      rSpark,
      rJDBC,
      rTiDB,
      skipJDBC,
      skipTiDB
    )
  }

  /** Run test with sql `qSpark` for TiSpark and TiDB, `qJDBC` for Spark-JDBC. Throw fail exception when
   *    - TiSpark query throws exception
   *    - Both TiDB and Spark-JDBC queries fails to execute
   *    - Both TiDB and Spark-JDBC results differ from TiSpark result
   *
   *  rSpark, rJDBC and rTiDB are used when we want to guarantee a fixed result which might change due to
   *    - Current incorrectness/instability in used version(s)
   *    - Format differences for partial data types
   *
   * @param qSpark    query for TiSpark and TiDB
   * @param qJDBC     query for Spark-JDBC
   * @param rSpark    pre-calculated TiSpark result
   * @param rJDBC     pre-calculated Spark-JDBC result
   * @param rTiDB     pre-calculated TiDB result
   * @param skipJDBC  whether not to run test for Spark-JDBC
   * @param skipTiDB  whether not to run test for TiDB
   */
  def runTest(qSpark: String,
              qJDBC: String,
              skipped: Boolean = false,
              rSpark: List[List[Any]] = null,
              rJDBC: List[List[Any]] = null,
              rTiDB: List[List[Any]] = null,
              skipJDBC: Boolean = false,
              skipTiDB: Boolean = false): Unit = {
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

    val isOrdered = qSpark.contains(" order by ")

    if (skipJDBC || !compResult(r1, r2, isOrdered)) {
      if (!skipTiDB && r3 == null) {
        try {
          r3 = queryTiDB(qSpark)
        } catch {
          case e: Throwable => logger.warn(s"TiDB failed when executing:$qSpark", e) // TiDB failed
        }
      }
      if (skipTiDB || !compResult(r1, r3, isOrdered)) {
        fail(
          s"Failed with \nTiSpark:\t\t${mapStringNestedList(r1)}\nSpark With JDBC:${mapStringNestedList(r2)}\nTiDB:\t\t\t${mapStringNestedList(r3)}"
        )
      }
    }
  }

  private def mapStringNestedList(result: List[List[Any]]): String =
    if (result == null) "null" else result.map(mapStringList).mkString(",")

  private def mapStringList(result: List[Any]): String =
    if (result == null) "null" else "List(" + result.map(mapString).mkString(",") + ")"

  private def mapString(result: Any): String = {
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
  }
}
