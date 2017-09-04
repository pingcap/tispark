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

import scala.collection.mutable.ArrayBuffer
import com.typesafe.scalalogging.slf4j.LazyLogging

class TestCase(val prop: Properties) extends LazyLogging {
  object RunMode extends Enumeration {
    type RunMode = Value
    val Test, Load, LoadNTest, Dump = Value
  }

  protected val KeyDumpDBList = "test.dumpDB.databases"
  protected val KeyMode = "test.mode"
  protected val KeyTestBasePath = "test.basepath"
  protected val KeyTestIgnore = "test.ignore"

  protected val dbNames = getOrElse(prop, KeyDumpDBList, "").split(",")
  protected val mode = RunMode.withName(getOrElse(prop, KeyMode, "Test"))
  protected val basePath = getOrElse(prop, KeyTestBasePath, "./testcases")
  protected val ignoreCases = getOrElse(prop, KeyTestIgnore, "").split(",")
  protected lazy val jdbc = new JDBCWrapper(prop)
  protected lazy val spark = new SparkWrapper(prop)

  logger.info("Databases to dump: " + dbNames.mkString(","))
  logger.info("Run Mode: " + mode)
  logger.info("basePath: " + basePath)

  def init(): Unit = {
    mode match {
      case RunMode.Dump => {
        dbNames.filter(!_.isEmpty).foreach { dbName =>
          logger.info("Dumping database " + dbName)
          if (dbName == null) {
            throw new IllegalArgumentException("database name is null while dumping")
          }
          jdbc.init(dbName)
          ensurePath(basePath, dbName)
          jdbc.dumpAllTables(joinPath(basePath, dbName))
        }
      }
      case RunMode.Load => {
        work(basePath, false, true)
      }
      case RunMode.Test => {
        work(basePath, true, false)
      }
      case RunMode.LoadNTest => {
        work(basePath, true, true)
      }
    }
  }

  protected def work(parentPath: String, run: Boolean, load: Boolean): Unit = {
    val ddls = ArrayBuffer.empty[String]
    val dataFiles = ArrayBuffer.empty[String]
    val dirs = ArrayBuffer.empty[String]

    val dir = new File(parentPath)
    val testCases = ArrayBuffer.empty[(String, String)]

    var dbName = dir.getName
    if (!ignoreCases.exists(_.equalsIgnoreCase(dbName))) {
      if (dir.isDirectory) {
        dir.listFiles().map { f =>
          if (f.isDirectory) {
            dirs.append(f.getAbsolutePath)
          } else {
            if (f.getName.endsWith(DDLSuffix)) {
              ddls.append(f.getAbsolutePath)
            } else if (f.getName.endsWith(DataSuffix)) {
              dataFiles.append(f.getAbsolutePath)
            } else if (f.getName.endsWith(SQLSuffix)) {
              testCases.append((f.getName, readFile(f.getAbsolutePath).mkString("\n")))
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
          logger.info("Resister for DDL script " + file)
          jdbc.createTable(file)
        }
        }
        dataFiles.foreach{ file => {
          logger.info("Resister for data loading script " + file)
          jdbc.loadTable(file)
        }
        }
      }
      if (run) {
        test(dbName, testCases)
      }

      dirs.foreach { dir =>
        work(dir, run, load)
      }
    }
  }

  def test(dbName: String, testCases: ArrayBuffer[(String, String)]) = {
    jdbc.init(dbName)
    spark.init(dbName)

    testCases.sortBy(_._1).foreach { case (file, sql) =>
      logger.info(s" Query TiSpark ${file} ")
      val actual: List[List[Any]] = time { spark.querySpark(sql) }(logger)
      logger.info(s" \nQuery TiDB ${file} ")
      val baseline: List[List[Any]] = time { jdbc.queryTiDB(sql)._2 }(logger)
      val result = compResult(actual, baseline)
      if (!result) {
        logger.info(s"Dump diff for TiSpark ${file} \n")
        writeResult(actual, file + ".result.spark")
        logger.info(s"Dump diff for TiDB ${file} \n")
        writeResult(baseline, file + ".result.tidb")
      }

      logger.info(s" \n*************** ${file} result: ${result}\n\n\n")
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
