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
import java.util.Properties

import com.pingcap.spark.Utils._
import com.typesafe.scalalogging.slf4j.LazyLogging

import scala.collection.mutable.ArrayBuffer

class TestCase(val prop: Properties) extends LazyLogging {

  object RunMode extends Enumeration {
    type RunMode = Value
    val Test, Load, LoadNTest, Dump, TestIndex, TestDAG = Value
  }

  protected val KeyDumpDBList = "test.dumpDB.databases"
  protected val KeyMode = "test.mode"
  protected val KeyTestBasePath = "test.basepath"
  protected val KeyTestIgnore = "test.ignore"

  protected val dbNames: Array[String] = getOrElse(prop, KeyDumpDBList, "").split(",")
  protected val mode: RunMode.RunMode = RunMode.withName(getOrElse(prop, KeyMode, "Test"))
  protected val basePath: String = getOrElse(prop, KeyTestBasePath, "./testcases")
  protected val ignoreCases: Array[String] = getOrElse(prop, KeyTestIgnore, "").split(",")
  protected lazy val jdbc = new JDBCWrapper(prop)
  protected lazy val spark = new SparkWrapper()

  protected var testsFailed = 0
  protected var testsExecuted = 0
  protected var testsSkipped = 0
  protected var inlineSQLNumber = 0

  private val tidbExceptionOutput = "TiDB execution failed with exception caught"
  private val sparkExceptionOutput = "Spark execution failed with exception caught"

  logger.info("Databases to dump: " + dbNames.mkString(","))
  logger.info("Run Mode: " + mode)
  logger.info("basePath: " + basePath)

  def init(): Unit = {
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

      case RunMode.Load => work(basePath, false, true, true)

      case RunMode.Test => work(basePath, true, false, true)

      case RunMode.LoadNTest => work(basePath, true, true, true)

      case RunMode.TestIndex => work(basePath, true, false, false)

      case RunMode.TestDAG => work(basePath, true, false, false)
    }

    mode match {
      case RunMode.Test | RunMode.TestIndex =>
        logger.warn("Result: All tests done.")
        logger.warn("Result: Tests run: " + testsExecuted
          + "  Tests succeeded: " + (testsExecuted - testsFailed - testsSkipped)
          + "  Tests failed: " + testsFailed
          + "  Tests skipped: " + testsSkipped)
      case _ =>
    }
  }

  protected def work(parentPath: String, run: Boolean, load: Boolean, compareWithTiDB: Boolean): Unit = {
    val ddls = ArrayBuffer.empty[String]
    val dataFiles = ArrayBuffer.empty[String]
    val dirs = ArrayBuffer.empty[String]

    val dir = new File(parentPath)
    val testCases = ArrayBuffer.empty[(String, String)]

    var dbName = dir.getName
    logger.info(s"get ignored: ${ignoreCases.toList}")
    logger.info(s"current dbName $dbName is " + (if (ignoreCases.exists(_.equalsIgnoreCase(dbName))) "" else "not ") + "ignored")

    logger.info(s"run=${run.toString} load=${load.toString} compareWithTiDB=${compareWithTiDB.toString}")
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
        test(dbName, testCases, compareWithTiDB)
      }

      dirs.foreach { dir =>
        work(dir, run, load, compareWithTiDB)
      }
    }
  }

  private def printDiff(sqlName: String, sql: String, TiDB: List[List[Any]], TiSpark: List[List[Any]]): Unit = {
    for (row <- TiSpark) {
      for (str <- row) {
        if (str != null &&
          (str.toString.contains("type mismatch") ||
            str.toString.contains("only support precision") ||
            str.toString.contains("Error converting access pointsnull"))) {
          return
        }
      }
    }
    for (row <- TiDB) {
      for (str <- row) {
        if (str != null &&
          (str.toString.contains("out of range") ||
            str.toString.contains("BIGINT") ||
            str.toString.contains("invalid time format"))) {
          return
        }
      }
    }
    logger.info(s"Dump diff for TiSpark $sqlName \n")
    writeResult(sql, TiDB, sqlName + ".result.tidb")
    logger.info(s"Dump diff for TiDB $sqlName \n")
    writeResult(sql, TiSpark, sqlName + ".result.spark")
  }

  def test(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    jdbc.init(dbName)
    spark.init(dbName)

    testCases.sortBy(_._1).foreach { case (file, sql) =>
      logger.info(s"Query TiSpark $file ")
      val actual = execSpark(sql)
      logger.info(s"\nQuery TiDB $file ")
      val baseline = execTiDB(sql)
      val result = compResult(actual, baseline)
      if (!result) {
        testsFailed += 1
        printDiff(s"$dbName.$file", sql, actual, baseline)
      }
      testsExecuted += 1

      logger.warn(s"\n*************** $file result: $result\n\n\n")
    }
  }

  def execTiDB(sql: String): List[List[Any]] = {
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

  def execSpark(sql: String): List[List[Any]] = {
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

  def execTiDBAndShow(str: String): Unit = {
    try {
      val tidb = execTiDB(str)
      logger.info(s"output: $tidb")
    } catch {
      case e: Exception =>
        logger.error(s"$tidbExceptionOutput: ${e.getMessage}\n")
    }
  }

  def execSparkAndShow(str: String): Unit = {
    try {
      val spark = execSpark(str)
      logger.info(s"output: $spark")
    } catch {
      case e: Exception =>
        logger.error(s"$sparkExceptionOutput: ${e.getMessage}\n")
    }
  }

  def execBothAndShow(str: String): Unit = {
    testsExecuted += 1
    inlineSQLNumber += 1
    execTiDBAndShow(str)
    execSparkAndShow(str)
  }

  def execBothAndSkip(str: String): Boolean = {
    execBothAndJudge(str, skipped = true)
  }

  def execBothAndJudge(str: String, skipped: Boolean = false): Boolean = {
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
      tidb = execTiDB(str)
    } catch {
      case e: Exception =>
        logger.error(s"$tidbExceptionOutput: ${e.getMessage}\n")
        tidb = List.apply(List.apply[String](e.getMessage))
        tidbRunTimeError = true
    }
    try {
      spark = execSpark(str)
    } catch {
      case e: Exception =>
        logger.error(s"$sparkExceptionOutput: ${e.printStackTrace()}\n")
        spark = List.apply(List.apply[String](e.getMessage))
        sparkRunTimeError = true
    }

    val isFalse = tidbRunTimeError || sparkRunTimeError || !compResult(tidb, spark)
    if (isFalse) {
      if (skipped) {
        logger.warn(s"TEST SKIPPED.\n")
      } else {
        logger.warn(s"TEST FAILED.\n")
      }
      logger.warn(s"TiDB output: $tidb")
      logger.warn(s"Spark output: $spark")
      if (!skipped) {
        testsFailed += 1
        printDiff(s"inlineTest$inlineSQLNumber", str, tidb, spark)
      } else {
        return false
      }
    } else {
      if (skipped) {
        logger.warn(s"TEST SKIPPED.\n")
      } else {
        logger.info(s"TEST PASSED.\n")
      }
    }
    isFalse
  }

  def run(dbName: String): Unit = {}

  private def testAndCalc(myTest: TestCase, dbName: String): Unit = {
    myTest.inlineSQLNumber = inlineSQLNumber
    myTest.run(dbName)
    testsExecuted += myTest.testsExecuted
    testsSkipped += myTest.testsSkipped
    testsFailed += myTest.testsFailed
    inlineSQLNumber = myTest.inlineSQLNumber
  }

  private def testInline(dbName: String): Unit = {
    if (dbName.equalsIgnoreCase("test_index")) {
      testAndCalc(new TestIndex(prop), dbName)
    } else if (dbName.equalsIgnoreCase("test_types")) {
      testAndCalc(new TestTypes(prop), dbName)
    } else if (dbName.equalsIgnoreCase("tispark_test")) {
      val colList = jdbc.getTableColumnNames("full_data_type_table")
      testAndCalc(new DAGTestCase(colList, prop), dbName)
    } else if (dbName.equalsIgnoreCase("test_null")) {
      testAndCalc(new TestNull(prop), dbName)
    }
  }

  private def test(dbName: String, testCases: ArrayBuffer[(String, String)], compareWithTiDB: Boolean): Unit = {
    if (compareWithTiDB) {
      test(dbName, testCases)
    } else {
      testInline(dbName)
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

    def compValue(lhs: Any, rhs: Any): Boolean = lhs match {
      case _: Double | _: Float | _: BigDecimal | _: java.math.BigDecimal =>
        Math.abs(toDouble(lhs) - toDouble(rhs)) < 0.01
      case _: Number | _: BigInt | _: java.math.BigInteger =>
        toInteger(lhs) == toInteger(rhs)
      case _ => lhs == rhs
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
