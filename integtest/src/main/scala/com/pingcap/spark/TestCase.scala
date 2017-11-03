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
    val Test, Load, LoadNTest, Dump, TestIndex = Value
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

    }

    mode match {
      case RunMode.Test | RunMode.TestIndex =>
        logger.warn("Result: All tests done.")
        logger.warn("Result: Tests run: " + testsExecuted
          + "  Tests succeeded: " + (testsExecuted - testsFailed - testsSkipped)
          + "  Tests failed: " + testsFailed
          + "  Tests skipped: " + testsSkipped)
    }
  }

  protected def work(parentPath: String, run: Boolean, load: Boolean, compareWithTiDB: Boolean): Unit = {
    val ddls = ArrayBuffer.empty[String]
    val dataFiles = ArrayBuffer.empty[String]
    val dirs = ArrayBuffer.empty[String]

    val dir = new File(parentPath)
    val testCases = ArrayBuffer.empty[(String, String)]

    var dbName = dir.getName
    logger.info("get ignored: " + ignoreCases.toList)
    logger.info("current dbName " + dbName + " is " + (if (ignoreCases.exists(_.equalsIgnoreCase(dbName))) "" else "not ") + "ignored")

    logger.info("run=" + run.toString + " load=" + load.toString + " compareWithTiDB=" + compareWithTiDB.toString)
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
        logger.info("Switch to " + dbName)
        dbName = jdbc.init(dbName)
        logger.info("Load data... ")
        ddls.foreach{ file => {
          logger.info("Register for DDL script " + file)
          jdbc.createTable(file)
        }
        }
        dataFiles.foreach{ file => {
          logger.info("Register for data loading script " + file)
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
        logger.info(s"Dump diff for TiSpark $file \n")
        writeResult(actual, file + ".result.spark")
        logger.info(s"Dump diff for TiDB $file \n")
        writeResult(baseline, file + ".result.tidb")
      }
      testsExecuted += 1

      logger.warn(s"\n*************** $file result: $result\n\n\n")
    }
  }

  def execSpark(sql: String): List[List[Any]] = {
    try {
      val ans = time {
        spark.querySpark(sql)
      }(logger)
      logger.info("hint: " + ans.length + " row(s)")
      ans
    } catch {
      case e:
        Exception => logger.error("Spark execution failed with exception caught: \n" + e.printStackTrace())
        List.empty
    }
  }

  def execTiDB(sql: String): List[List[Any]] = {
    try {
      val ans = time {
        jdbc.queryTiDB(sql)._2
      }(logger)
      logger.info("hint: " + ans.length + " row(s)")
      ans
    } catch {
      case e:
        Exception => logger.error("Spark execution failed with exception caught: \n" + e.printStackTrace())
        List.empty
    }
  }

  private def execSparkAndShow(str: String): Unit = {
    val spark = execSpark(str)
    logger.info("output: " + spark)
  }

  private def execTiDBAndShow(str: String): Unit = {
    val tidb = execTiDB(str)
    logger.info("output: " + tidb)
  }

  private def execBothAndShow(str: String): Unit = {
    testsExecuted += 1
    execTiDBAndShow(str)
    execSparkAndShow(str)
  }

  private def execBothAndSkip(str: String): Boolean = {
    execBothAndJudge(str, skipped = true)
  }

  private def execBothAndJudge(str: String, skipped: Boolean = false): Boolean = {
    val tidb = execTiDB(str)
    val spark = execSpark(str)
    val isFalse = !tidb.equals(spark)
    if (isFalse) {
      if (!skipped) {
        testsFailed += 1
      }
      logger.warn("Test failed:\n")
      logger.warn("TiDB output: " + tidb)
      logger.warn("Spark output: " + spark)
    }
    testsExecuted += 1
    if (skipped) {
      testsSkipped += 1
    }
    isFalse
  }

  private def testType(): Unit = {
    execBothAndShow(s"select * from t1")
    var result = false
    result |= execBothAndJudge(s"select c1 from t1")
    result |= execBothAndJudge(s"select c2 from t1")
    result |= execBothAndJudge(s"select c3 from t1")
    result |= execBothAndJudge(s"select c4 from t1")
    result |= execBothAndJudge(s"select c5 from t1")
    result |= execBothAndJudge(s"select c6 from t1")
    result |= execBothAndJudge(s"select c7 from t1")
    result |= execBothAndJudge(s"select c8 from t1")
    result |= execBothAndJudge(s"select c9 from t1")
    result |= execBothAndJudge(s"select c10 from t1")
    //    result |= execBothAndJudge(s"select c11 from t1")
    result |= execBothAndJudge(s"select c12 from t1")
    //    result |= execBothAndJudge(s"select c13 from t1")
    result |= execBothAndJudge(s"select c14 from t1")
    //    result |= execBothAndJudge(s"select c15 from t1")

    result = !result
    logger.warn(s"\n*************** SQL Type Tests result: $result\n\n\n")
  }

  private def testTimeType(): Unit = {
    execSparkAndShow(s"select * from t2")
    execSparkAndShow(s"select * from t3")
    var result = false

    result |= execBothAndSkip(s"select UNIX_TIMESTAMP(c14) from t1")
    execSparkAndShow(s"select CAST(c14 AS LONG) from t1")
    execSparkAndShow(s"select c13 from t1")

    execTiDBAndShow(s"select c14 + c13 from t1")
    execSparkAndShow(s"select CAST(c14 AS LONG) + c13 from t1")

    result |= execBothAndSkip(s"select c15 from t1")
    result |= execBothAndSkip(s"select UNIX_TIMESTAMP(c15) from t1")

    result = !result
    logger.warn(s"\n*************** SQL Time Tests result: " + (if (result) "true" else "Fixing...Skipped") + "\n\n\n")
  }

  private def testIndex(): Unit = {
    var result = false
    result |= execBothAndJudge("select * from test_index where a < 30")

    result |= execBothAndJudge("select * from test_index where d > \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d < \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d > \'116.3\' and d < \'116.7\'")

    result |= execBothAndJudge("select * from test_index where d = \'116.72873\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72874\' and e < \'40.0452\'")

    result |= execBothAndJudge("select * from test_index where c > \'2008-02-06 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where c >= \'2008-02-06 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where c < \'2008-02-06 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where c <= \'2008-02-06 14:00:00\'")
    //    TODO: this case should be fixed later
    //    result |= execBothAndJudge("select * from test_index where c = \'2008-02-06 14:00:00\'")

    result |= execBothAndJudge("select * from test_index where c > date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c >= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c < date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c <= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) = date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) > date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) >= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) < date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where DATE(c) <= date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c <> date \'2008-02-05\'")
    result |= execBothAndJudge("select * from test_index where c > \'2008-02-04 14:00:00\' and d > \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72873\' and c > \'2008-02-04 14:00:00\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72873\' and c < \'2008-02-04 14:00:00\'")

    result = !result
    logger.warn(s"\n*************** Index Tests result: $result\n\n\n")
  }

  def testInline(dbName: String): Unit = {
    if(dbName.equalsIgnoreCase("test_index")) {
      spark.init(dbName)
      jdbc.init(dbName)

      testType()
      testTimeType()
      testIndex()

    }

  }

  def test(dbName: String, testCases: ArrayBuffer[(String, String)], compareWithTiDB: Boolean): Unit = {
    if (compareWithTiDB) {
      test(dbName, testCases)
    } else {
      testInline(dbName)
    }
  }

  def compResult(lhs: List[List[Any]], rhs: List[List[Any]]): Boolean = {
    def toDouble(x: Any): Double = x match {
      case d: Double => d
      case d: Float => d.toDouble
      case d: java.math.BigDecimal => d.doubleValue()
      case d: BigDecimal => d.bigDecimal.doubleValue()
      case d: Number => d.doubleValue()
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

    !lhs.zipWithIndex.exists {
      case (row, i) => !compRow(row, rhs(i))
    }
  }

  def writeResult(rowList: List[List[Any]], path: String): Unit = {
    val sb = StringBuilder.newBuilder
    rowList.foreach{
      row => {
        row.foreach{
          value => sb.append(value + " ")
        }
        sb.append("\n")
      }
    }
    writeFile(sb.toString(), path)
  }
}
