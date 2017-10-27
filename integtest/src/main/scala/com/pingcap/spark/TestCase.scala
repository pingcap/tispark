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

  def ExecWithSparkResult(sql: String): List[List[Any]] = {
    time {
      spark.querySpark(sql)
    }(logger)
  }

  def ExecWithTiDBResult(sql: String): List[List[Any]] = {
    time {
      jdbc.queryTiDB(sql)._2
    }(logger)
  }

  def test(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    jdbc.init(dbName)
    spark.init(dbName)

    testCases.sortBy(_._1).foreach { case (file, sql) =>
      logger.info(s" Query TiSpark $file ")
      val actual = ExecWithSparkResult(sql)
      logger.info(s" \nQuery TiDB $file ")
      val baseline = ExecWithTiDBResult(sql)
      val result = compResult(actual, baseline)
      if (!result) {
        logger.info(s"Dump diff for TiSpark $file \n")
        writeResult(actual, file + ".result.spark")
        logger.info(s"Dump diff for TiDB $file \n")
        writeResult(baseline, file + ".result.tidb")
      }

      logger.info(s" \n*************** $file result: $result\n\n\n")
    }
  }

  def testInline(): Unit = {
    spark.init("test_index")
    var actual = ExecWithSparkResult(s" select * from t2")
    logger.info("result:" + actual)
    actual = ExecWithSparkResult(s" select * from t3")
    logger.info("result:" + actual)
    actual = ExecWithSparkResult(s" select * from t1")
    logger.info("result:" + actual)
  }

  def test(dbName: String, testCases: ArrayBuffer[(String, String)], compareWithTiDB: Boolean): Unit = {
    if (compareWithTiDB) {
      test(dbName, testCases)
    } else {
      testInline()
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
