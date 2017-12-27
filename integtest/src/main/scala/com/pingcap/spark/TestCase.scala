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

package com.pingcap.spark

import java.io.File
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties

import com.pingcap.spark.Utils._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable.ArrayBuffer

class TestCase(val prop: Properties) extends LazyLogging {

  object RunMode extends Enumeration {
    type RunMode = Value
    val Test, Load, LoadNTest, Dump, TestIndex, TestDAG, SqlOnly = Value
  }

  protected val KeyDumpDBList = "test.dumpDB.databases"
  protected val KeyMode = "test.mode"
  protected val KeyTestBasePath = "test.basepath"
  protected val KeyTestIgnore = "test.ignore"
  protected val KeyTestDBToUse = "test.db"
  protected val KeyTestSql = "test.sql"

  protected val dbNames: Array[String] = getOrElse(prop, KeyDumpDBList, "").split(",")
  protected val sqlCheck: String = getOrElse(prop, KeyTestSql, "")
  protected val oneSqlOnly: Boolean = sqlCheck != ""
  protected val mode: RunMode.RunMode = if (oneSqlOnly) RunMode.SqlOnly else RunMode.withName(getOrElse(prop, KeyMode, "Test"))
  protected val basePath: String = getOrElse(prop, KeyTestBasePath, "./testcases")
  protected val ignoreCases: Array[String] = getOrElse(prop, KeyTestIgnore, "").split(",")
  protected val useDatabase: Array[String] = getOrElse(prop, KeyTestDBToUse, "").split(",")
  protected val dbAssigned: Boolean = !useDatabase.isEmpty && !useDatabase.head.isEmpty

  protected lazy val jdbc = new JDBCWrapper(prop)
  protected lazy val spark = new SparkWrapper()
  protected lazy val spark_jdbc = new SparkJDBCWrapper(prop)

  private val eps = 1.0e-2

  @volatile protected var testsFailed = 0
  @volatile protected var testsExecuted = 0
  @volatile protected var testsSkipped = 0
  @volatile protected var inlineSQLNumber = 0
  @volatile protected var ignoredTest = 0

  class SQLProcessorRunnable(sql: String, sqlNumber: Integer, dbName: String) extends Runnable {
    override def run(): Unit = {
      execAllAndJudge(sql, sqlNumber)
    }
  }

  private val tidbExceptionOutput = "TiDB execution failed with exception caught"
  private val sparkExceptionOutput = "Spark execution failed with exception caught"
  private val sparkJDBCExceptionOutput = "Spark JDBC execution failed with exception caught"

  private final val SparkIgnore = Set[String](
    "type mismatch",
    "only support precision",
    "Invalid Flag type for TimestampType: 8",
    "Invalid Flag type for DateTimeType: 8",
    "Decimal scale (18) cannot be greater than precision ",
    "0E-11", // unresolvable precision fault
    "overflows",
    "2017-01-01" // timestamp error
    //    "unknown error Other"
    //    "Error converting access pointsnull"
  )

  private final val TiDBIgnore = Set[String](
//    "out of range"
//    "BIGINT",
//    "invalid time format",
//    "line 1 column 13 near"
  )

  protected val compareOpList = List("=", "<", ">", "<=", ">=", "!=", "<>")
  protected val arithmeticOpList = List("+", "-", "*", "/", "%")
  protected val LEFT_TB_NAME = "A"
  protected val RIGHT_TB_NAME = "B"
  protected val TABLE_NAME = "full_data_type_table"
  protected val LITERAL_NULL = "null"
  protected val SCALE_FACTOR: Integer = 4 * 4
  protected val ID_COL = "id_dt"

  def init(): Unit = {

    logger.info("Databases to dump: " + dbNames.mkString(","))
    logger.info("Run Mode: " + mode)
    logger.info("basePath: " + basePath)
    logger.info("use these DataBases only: " + (if (dbAssigned) useDatabase.head else "None"))

    mode match {
      case RunMode.Dump => dbNames.filter(!_.isEmpty).foreach { dbName =>
        logger.info("Dumping database " + dbName)
        if (dbName == null) {
          throw new IllegalArgumentException("database name is null while dumping")
        }
        jdbc.init(dbName)
        ensurePath(basePath, dbName)
        jdbc.dumpAllTables(joinPath(basePath, dbName))
      }

      case RunMode.Load => work(basePath, run=false, load=true, compareNeeded=true)

      case RunMode.Test => work(basePath, run=true, load=false, compareNeeded=true)

      case RunMode.LoadNTest => work(basePath, run=true, load=true, compareNeeded=true)

      case RunMode.TestIndex => work(basePath, run=true, load=false, compareNeeded=false)

      case RunMode.TestDAG => work(basePath, run=true, load=false, compareNeeded=false)

      case RunMode.SqlOnly => work(basePath, run=true, load=false, compareNeeded=false)
    }

    mode match {
      case RunMode.Test | RunMode.TestIndex | RunMode.TestDAG =>
        logger.warn("Result: All tests done.")
        logger.warn("Result: Tests run: " + testsExecuted
          + "  Tests succeeded: " + (testsExecuted - testsFailed - testsSkipped)
          + "  Tests failed: " + testsFailed
          + "  Tests skipped: " + testsSkipped)
        jdbc.close()
        spark.close()
        spark_jdbc.close()
      case _ =>
    }
  }

  protected def work(parentPath: String, run: Boolean, load: Boolean, compareNeeded: Boolean): Unit = {
    val ddls = ArrayBuffer.empty[String]
    val dataFiles = ArrayBuffer.empty[String]
    val dirs = ArrayBuffer.empty[String]

    val dir = new File(parentPath)
    val testCases = ArrayBuffer.empty[(String, String)]

    var dbName = dir.getName
    logger.info(s"get ignored: ${ignoreCases.toList}")
    logger.info(s"current dbName $dbName is " + (if (ignoreCases.exists(_.equalsIgnoreCase(dbName))
      || (dbAssigned && !useDatabase.exists(_.equalsIgnoreCase(dbName)))) "" else "not ") +
      "ignored")

    logger.info(s"run=${run.toString} load=${load.toString} compareNeeded=${compareNeeded.toString}")
    try {
      if (!ignoreCases.exists(_.equalsIgnoreCase(dbName))) {
        if (dir.isDirectory) {
          dir.listFiles().map { f =>
            if (f.isDirectory) {
              dirs += f.getAbsolutePath
            } else {
              if (f.getName.endsWith(DDLSuffix)) {
                ddls += f.getAbsolutePath
              } else if (f.getName.endsWith(DataSuffix)) {
                dataFiles += f.getAbsolutePath
              } else if (f.getName.endsWith(SQLSuffix)) {
                testCases += ((f.getName, readFile(f.getAbsolutePath).mkString("\n")))
              }
            }
          }
        } else {
          throw new IllegalArgumentException("Cannot prepare non-folder")
        }

        if (load) {
          logger.info(s"Switch to $dbName")
          dbName = jdbc.init(dbName)
          logger.info("Load data... ")
          ddls.foreach { file => {
            logger.info(s"Register for DDL script $file")
            jdbc.createTable(file)
          }
          }
          dataFiles.foreach { file => {
            logger.info(s"Register for data loading script $file")
            jdbc.loadTable(file)
          }
          }
        }
        if (run) {
          if (!dbAssigned || useDatabase.exists(_.equalsIgnoreCase(dbName))) {
            test(dbName, testCases, compareNeeded)
          }
        }

        dirs.foreach { dir =>
          work(dir, run, load, compareNeeded)
        }
      }
    } catch {
      case e: Exception => logger.error("Unexpected error occured: " + e.getMessage)
    }
  }

  private def printDiffSparkJDBC(sqlName: String, sql: String, sparkJDBC: List[List[Any]], tiSpark: List[List[Any]]): Unit = {
    if (compResult(sparkJDBC, tiSpark)) {
      return
    }

    try {
      logger.info(s"Dump diff for JDBC Spark $sqlName \n")
      writeResult(sql, sparkJDBC, sqlName + ".result.jdbc")
      logger.info(s"Dump diff for TiSpark $sqlName \n")
      writeResult(sql, tiSpark, sqlName + ".result.spark")
    } catch {
      case e : Exception => logger.error("Write file error:" + e.getMessage)
    }
  }

  private def printDiff(sqlName: String, sql: String, tiDb: List[List[Any]], tiSpark: List[List[Any]]): Unit = {
    if (compResult(tiDb, tiSpark)) {
      return
    }

    try {
      logger.info(s"Dump diff for TiSpark $sqlName \n")
      writeResult(sql, tiDb, sqlName + ".result.tidb")
      logger.info(s"Dump diff for TiDB $sqlName \n")
      writeResult(sql, tiSpark, sqlName + ".result.spark")
    } catch {
      case e: Exception => logger.error("Write file error:" + e.getMessage)
    }
  }

  def checkIgnore(value: Any, str: String): Boolean = {
    if (value == null) {
      false
    } else {
      value.toString.contains(str)
    }
  }

  def checkSparkIgnore(tiSpark: List[List[Any]]): Boolean = {
    val ignoreCase = SparkIgnore ++ TiDBIgnore
    tiSpark.exists(
      (row: List[Any]) => row.exists(
        (str: Any) => ignoreCase.exists(
          (i: String) => checkIgnore(str, i)
        )))
  }

  def checkTiDBIgnore(tiDb: List[List[Any]]): Boolean = {
    tiDb.exists(
      (row: List[Any]) => row.exists(
        (str: Any) => TiDBIgnore.exists(
          (i: String) => checkIgnore(str, i)
        )))
  }

  def checkSparkJDBCIgnore(sparkJDBC: List[List[Any]]): Boolean = {
    sparkJDBC.exists(
      (row: List[Any]) => row.exists(
        (str: Any) => SparkIgnore.exists(
          (i: String) => checkIgnore(str, i)
        )))
  }

  def testSparkAndSparkJDBC(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    spark_jdbc.init(dbName)
    spark.init(dbName)

    testCases.sortBy(_._1).foreach { case (file, sql) =>
      logger.info(s"\nquery on Spark $file ")
      val spark_jdbc = execSparkJDBC(sql)
      logger.info(s"query on TiSpark $file ")
      val spark = execSpark(sql)
      val result = compResult(spark_jdbc, spark)
      if (!result) {
        testsFailed += 1
        printDiffSparkJDBC(s"$dbName.$file", sql, spark_jdbc, spark)
      }
      testsExecuted += 1

      logger.warn(s"\n*************** $file result: $result\n\n\n")
    }
  }

  def test(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    jdbc.init(dbName)
    spark.init(dbName)

    testCases.sortBy(_._1).foreach { case (file, sql) =>
      logger.info(s"query on TiSpark $file ")
      val spark = execSpark(sql)
      logger.info(s"\nquery on TiDB $file ")
      val tidb = execTiDB(sql)
      val result = compResult(tidb, spark)
      if (!result) {
        printDiff(s"$dbName.$file", sql, tidb, spark)
      }
      testsExecuted += 1

      logger.warn(s"\n*************** $file result: $result\n\n\n")
    }
  }

  def execTiDB(sql: String, jdbc: JDBCWrapper = jdbc): List[List[Any]] = {
    try {
      val ans = time {
        jdbc.queryTiDB(sql)._2
      }(logger)
      logger.info(s"hint: ${ans.length} row(s)")
      ans
    } catch {
      case e: Exception => throw e
    }
  }

  def execSparkJDBC(sql: String, spark_jdbc: SparkJDBCWrapper = spark_jdbc): List[List[Any]] = {
    try {
      val ans = time {
        spark_jdbc.querySpark(sql)
      }(logger)
      logger.info(s"hint: ${ans.length} row(s)")
      ans
    } catch {
      case e: Exception => throw e
    }
  }

  def execSpark(sql: String, spark: SparkWrapper = spark): List[List[Any]] = {
    try {
      val ans = time {
        spark.querySpark(sql)
      }(logger)
      logger.info(s"hint: ${ans.length} row(s)")
      ans
    } catch {
      case e: Exception => throw e
    }
  }

  def execTiDBAndShow(str: String, jdbcWrapper: JDBCWrapper = jdbc): Unit = {
    try {
      val tidb = execTiDB(str, jdbcWrapper)
      logger.info(s"output: $tidb")
    } catch {
      case e: Exception =>
        logger.error(s"$tidbExceptionOutput: ${e.getMessage}\n")
    }
  }

  def execSparkJDBCAndShow(str: String, sparkJDBCWrapper: SparkJDBCWrapper = spark_jdbc): Unit = {
    try {
      val spark_jdbc = execSparkJDBC(str, sparkJDBCWrapper)
      logger.info(s"output: $spark_jdbc")
    } catch {
      case e: Exception =>
        logger.error(s"$sparkJDBCExceptionOutput: ${e.getMessage}\n")
    }
  }

  def execSparkAndShow(str: String, sparkWrapper: SparkWrapper = spark): Unit = {
    try {
      val spark = execSpark(str, sparkWrapper)
      logger.info(s"output: $spark")
    } catch {
      case e: Exception =>
        logger.error(s"$sparkExceptionOutput: ${e.getMessage}\n")
    }
  }

  def execBothSparkAndShow(str: String,
                           sparkWrapper: SparkWrapper = spark,
                           sparkJDBCWrapper: SparkJDBCWrapper = spark_jdbc): Unit = {
    testsExecuted += 1
    inlineSQLNumber += 1
    execSparkJDBCAndShow(str, sparkJDBCWrapper)
    execSparkAndShow(str, sparkWrapper)
  }

  def execBothAndShow(str: String,
                      jDBCWrapper: JDBCWrapper = jdbc,
                      sparkWrapper: SparkWrapper = spark): Unit = {
    testsExecuted += 1
    inlineSQLNumber += 1
    execTiDBAndShow(str, jDBCWrapper)
    execSparkAndShow(str, sparkWrapper)
  }

  def execAllAndShow(str: String,
                     jdbcWrapper: JDBCWrapper = jdbc,
                     sparkWrapper: SparkWrapper = spark,
                     sparkJDBCWrapper: SparkJDBCWrapper = spark_jdbc): Unit = {
    testsExecuted += 1
    inlineSQLNumber += 1
    execTiDBAndShow(str, jdbcWrapper)
    execSparkJDBCAndShow(str, sparkJDBCWrapper)
    execSparkAndShow(str, sparkWrapper)
  }

  def execSparkBothAndSkip(str: String,
                           sparkWrapper: SparkWrapper = spark,
                           sparkJDBCWrapper: SparkJDBCWrapper = spark_jdbc): Boolean = {
    execSparkBothAndJudge(str, sparkWrapper, sparkJDBCWrapper, skipped = true)
  }

  def execBothAndSkip(str: String,
                      jDBCWrapper: JDBCWrapper = jdbc,
                      sparkWrapper: SparkWrapper = spark): Boolean = {
    execBothAndJudge(str, jDBCWrapper, sparkWrapper, skipped = true)
  }

  def execSparkBothAndJudge(str: String,
                            sparkWrapper: SparkWrapper = spark,
                            sparkJDBCWrapper: SparkJDBCWrapper = spark_jdbc,
                            skipped: Boolean = false): Boolean = {
    var spark_jdbc: List[List[Any]] = List.empty
    var spark: List[List[Any]] = List.empty

    testsExecuted += 1
    if (skipped) {
      testsSkipped += 1
    } else {
      inlineSQLNumber += 1
    }
    var sparkJDBCRunTimeError = false
    var sparkRunTimeError = false

    try {
      spark_jdbc = execSparkJDBC(str, sparkJDBCWrapper)
    } catch {
      case e: Exception =>
        logger.error(s"$sparkJDBCExceptionOutput: ${e.getMessage}\n")
        spark_jdbc = List.apply(List.apply[String](e.getMessage))
        sparkJDBCRunTimeError = true
    }
    try {
      spark = execSpark(str, sparkWrapper)
    } catch {
      case e: Exception =>
        logger.error(s"$sparkExceptionOutput: ${e.getMessage}\n")
        spark = List.apply(List.apply[String](e.getMessage))
        sparkRunTimeError = true
    }
    val isFalse = sparkJDBCRunTimeError || sparkRunTimeError || !compResult(spark_jdbc, spark)
    if (isFalse) {
      if (skipped) {
        logger.warn(s"Test SKIPPED. #$inlineSQLNumber\n")
      } else {
        logger.warn(s"Test FAILED. #$inlineSQLNumber\n")
      }
      logger.warn(s"Spark-JDBC output: $spark_jdbc")
      logger.warn(s"Spark output: $spark")
      if (!skipped) {
        if (checkSparkIgnore(spark) || checkSparkJDBCIgnore(spark_jdbc))
        printDiffSparkJDBC(s"inlineTest$inlineSQLNumber", str, spark_jdbc, spark)
      } else {
        return false
      }
    } else {
      if (skipped) {
        logger.warn(s"Test SKIPPED. #$inlineSQLNumber\n")
      } else {
        logger.info(s"Test PASSED. #$inlineSQLNumber\n")
      }
    }
    isFalse
  }

  def execBothAndJudge(str: String,
                       jDBCWrapper: JDBCWrapper = jdbc,
                       sparkWrapper: SparkWrapper = spark,
                       skipped: Boolean = false): Boolean = {
    var tidb: List[List[Any]] = List.empty
    var spark: List[List[Any]] = List.empty

    testsExecuted += 1
    if (skipped) {
      testsSkipped += 1
    } else {
      inlineSQLNumber += 1
    }

    var tidbRunTimeError = false
    var sparkRunTimeError = false

    try {
      tidb = execTiDB(str, jDBCWrapper)
    } catch {
      case e: Exception =>
        logger.error(s"$tidbExceptionOutput: ${e.getMessage}\n")
        tidb = List.apply(List.apply[String](e.getMessage))
        tidbRunTimeError = true
    }
    try {
      spark = execSpark(str, sparkWrapper)
    } catch {
      case e: Exception =>
        logger.error(s"$sparkExceptionOutput: ${e.getMessage}\n")
        spark = List.apply(List.apply[String](e.getMessage))
        sparkRunTimeError = true
    }

    val isFalse = tidbRunTimeError || sparkRunTimeError || !compResult(tidb, spark)
    if (isFalse) {
      if (skipped) {
        logger.warn(s"Test SKIPPED. #$inlineSQLNumber\n")
      } else {
        logger.warn(s"Test FAILED. #$inlineSQLNumber\n")
      }
      logger.warn(s"TiDB output: $tidb")
      logger.warn(s"Spark output: $spark")
      if (!skipped) {
        printDiff(s"inlineTest$inlineSQLNumber", str, tidb, spark)
      } else {
        return false
      }
    } else {
      if (skipped) {
        logger.warn(s"Test SKIPPED. #$inlineSQLNumber\n")
      } else {
        logger.info(s"Test PASSED. #$inlineSQLNumber\n")
      }
    }
    isFalse
  }

  def execAllAndJudge(str: String,
                      sqlNumber: Integer,
                      jdbcWrapper: JDBCWrapper = jdbc,
                      sparkWrapper: SparkWrapper = spark,
                      sparkJDBCWrapper: SparkJDBCWrapper = spark_jdbc,
                      skipped: Boolean = false): (List[List[Any]], List[List[Any]], List[List[Any]], Boolean) = {
    var tidb: List[List[Any]] = List.empty
    var spark_jdbc: List[List[Any]] = List.empty
    var spark: List[List[Any]] = List.empty

    var tidbRunTimeError = false
    var sparkRunTimeError = false
    var sparkJDBCRunTimeError = false

    try {
      spark_jdbc = execSparkJDBC(str, sparkJDBCWrapper)
    } catch {
      case e: Exception =>
        logger.error(s"$sparkJDBCExceptionOutput: ${e.getMessage}\n")
        spark_jdbc = List.apply(List.apply[String](e.getMessage))
        sparkJDBCRunTimeError = true
    }
    try {
      spark = execSpark(str, sparkWrapper)
    } catch {
      case e: Exception =>
        logger.error(s"$sparkExceptionOutput: ${e.getMessage}\n")
        spark = List.apply(List.apply[String](e.getMessage))
        sparkRunTimeError = true
    }

    val isSparkJDBCvsSparkFalse = sparkRunTimeError || sparkJDBCRunTimeError || !compResult(spark_jdbc, spark)

    var isTiDBvsSparkFalse = false

    var isFalse = false

    if (isSparkJDBCvsSparkFalse) {
      try {
        tidb = execTiDB(str, jdbcWrapper)
      } catch {
        case e: Exception =>
          logger.error(s"$tidbExceptionOutput: ${e.getMessage}\n")
          tidb = List.apply(List.apply[String](e.getMessage))
          tidbRunTimeError = true
      }
      isTiDBvsSparkFalse = sparkRunTimeError || tidbRunTimeError || !compResult(tidb, spark)
      isFalse = isTiDBvsSparkFalse && isSparkJDBCvsSparkFalse
    }

    if (isFalse) {
      if (skipped) {
        logger.warn(s"Test SKIPPED. #$sqlNumber\n")
      }
      if (isTiDBvsSparkFalse) {
        logger.warn(s"TiDB output: $tidb")
      }
      if (isSparkJDBCvsSparkFalse) {
        logger.warn(s"Spark-JDBC output: $spark_jdbc")
      }
      logger.warn(s"Spark output: $spark")
      if (!skipped) {
        if (checkTiDBIgnore(tidb) || checkSparkIgnore(spark) || checkSparkJDBCIgnore(spark_jdbc)) {
          testsSkipped += 1
          logger.warn(s"Test SKIPPED. #$sqlNumber\n")
        } else {
          testsFailed += 1
          printDiffSparkJDBC(s"inlineTest$sqlNumber", str, spark_jdbc, spark)
          printDiff(s"inlineTest$sqlNumber", str, tidb, spark)
          logger.warn(s"Test FAILED. #$sqlNumber\n")
        }
      } else {
        return (List.empty, List.empty, List.empty, false)
      }
    } else {
      if (skipped) {
        logger.warn(s"Test SKIPPED. #$sqlNumber\n")
      } else {
        logger.info(s"Test PASSED. #$sqlNumber\n")
      }
    }
    (tidb, spark, spark_jdbc, isFalse)
  }

  def run(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {}

  private def testAndCalc(myTest: TestCase, dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    myTest.inlineSQLNumber = inlineSQLNumber
    myTest.run(dbName, testCases)
    testsExecuted += myTest.testsExecuted
    testsSkipped += myTest.testsSkipped
    testsFailed += myTest.testsFailed
    inlineSQLNumber = myTest.inlineSQLNumber
  }

  private def testInline(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    if (dbName.equalsIgnoreCase("test_index")) {
      testAndCalc(new IssueTestCase(prop), dbName, testCases)
      testAndCalc(new TestIndex(prop), dbName, testCases)
    } else if (dbName.equalsIgnoreCase("tispark_test")) {
      testAndCalc(new DAGTestCase(prop), dbName, testCases)
    } else {
      test(dbName, testCases)
    }
  }

  private def testSql(dbName: String, sql: String): Unit = {
    spark.init(dbName)
    spark_jdbc.init(dbName)
    logger.info(if (execSparkBothAndJudge(sql)) "TEST FAILED." else "TEST PASSED.")
  }

  private def test(dbName: String, testCases: ArrayBuffer[(String, String)], compareNeeded: Boolean): Unit = {
    try {
      if (oneSqlOnly) {
        testSql(dbName, sqlCheck)
      } else if (dbName.equalsIgnoreCase("tpch_test")) {
        testSparkAndSparkJDBC(dbName, testCases)
      } else if (compareNeeded) {
        test(dbName, testCases)
      } else {
        testInline(dbName, testCases)
      }
    } catch {
      case e: Exception => logger.error(s"Exception caught when testing on $dbName: " + e.getMessage)
    }
  }

  private def compResult(lhs: List[List[Any]], rhs: List[List[Any]]): Boolean = {
    def toDouble(x: Any): Double = x match {
      case d: Double => d
      case d: Float => d.toDouble
      case d: java.math.BigDecimal => d.doubleValue()
      case d: BigDecimal => d.bigDecimal.doubleValue()
      case d: Number => d.doubleValue()
      case _ => 0.0
    }

    def toInteger(x: Any): Long = x match {
      case d: BigInt => d.bigInteger.longValue()
      case d: Number => d.longValue()
    }

    def toString(value: Any): String = {
      new SimpleDateFormat("yy-MM-dd HH:mm:ss").format(value)
    }

    def compValue(lhs: Any, rhs: Any): Boolean = {
      if (lhs == rhs || lhs.toString == rhs.toString) {
        true
      } else lhs match {
        case _: Double | _: Float | _: BigDecimal | _: java.math.BigDecimal =>
          val l = toDouble(lhs)
          val r = toDouble(rhs)
          Math.abs(l - r) < eps || Math.abs(r) > eps && Math.abs((l - r) / r) < eps
        case _: Number | _: BigInt | _: java.math.BigInteger =>
          toInteger(lhs) == toInteger(rhs)
        case _: Timestamp =>
          toString(lhs) == toString(rhs)
        case _ =>
          false
      }
    }

    def compRow(lhs: List[Any], rhs: List[Any]): Boolean = {
      if (lhs == null && rhs == null) {
        true
      } else if (lhs == null || rhs == null) {
        false
      } else {
        !lhs.zipWithIndex.exists {
          case (value, i) => !compValue(value, rhs(i))
        }
      }
    }

    try {
      !lhs.zipWithIndex.exists {
        case (row, i) => !compRow(row, rhs(i))
      }
    } catch {
      // TODO:Remove this temporary exception handling
      //      case _:RuntimeException => false
      case _: Throwable => false
    }
  }

  private def writeResult(sql: String, rowList: List[List[Any]], path: String): Unit = {
    val sb = StringBuilder.newBuilder
    sb.append(sql + "\n")
    rowList.foreach {
      row => {
        row.foreach {
          value => sb.append(value + " ")
        }
        sb.append("\n")
      }
    }
    writeFile(sb.toString(), path)
  }
}
