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

class TestCase(val prop: Properties) {
  protected val dbName = prop.getProperty("database")
  protected val dumpDB = Option(prop.getProperty("dumpDB")).getOrElse("false").toBoolean
  protected val basePath = getOrThrow(prop, "testbasepath")
  protected lazy val jdbc = new JDBCWrapper(prop)
  protected lazy val spark = new SparkWrapper(prop)


  protected val testCases = ArrayBuffer.empty[String]

  def testName(): String = getClass.getSimpleName

  def init(): Unit = {
    if (dumpDB) {
      if (dbName == null) {
        throw new IllegalArgumentException("database name is null while dumping")
      }
      jdbc.init(dbName)
      ensurePath(basePath, dbName)
      jdbc.dumpAllTables(joinPath(basePath, dbName))
    } else {
      jdbc.init(dbName)
      prepareTestcases(basePath)
    }
  }

  protected def prepareTestcases(parentPath: String): Unit = {
    val ddls = ArrayBuffer.empty[String]
    val dataFiles = ArrayBuffer.empty[String]

    val dir = new File(parentPath)
    if (dir.isDirectory) {
      dir.listFiles().map {f =>
        if (f.isDirectory) {
          prepareTestcases(f.getAbsolutePath)
        } else {
          if (f.getName.endsWith(DDLSuffix)) {
            ddls.append(f.getAbsolutePath)
          } else if (f.getName.endsWith(DataSuffix)) {
            dataFiles.append(f.getAbsolutePath)
          } else if (f.getName.endsWith(SQLSuffix)) {
            testCases.append(Utils.readFile(f.getAbsolutePath).mkString("\n"))
          }
        }
      }
    }

    ddls.foreach(jdbc.createTable)
    dataFiles.foreach(jdbc.loadTable)
  }

  def test() = {
    testCases.foreach { sql =>
      println("================= Query TiSpark =================\n")
      val actual: List[List[Any]] = spark.querySpark(sql)
      println("================= Query TiDB =================\n")
      val baseline: List[List[Any]] = jdbc.queryTiDB(sql)._2

      val result = compResult(actual, baseline)
      if (!result) {
        println("================= TiSpark =================\n")
        printResult(actual)
        println("================= TiDB =================\n")
        printResult(baseline)
      }

      println(testName + " result: " + result)
    }
  }

  def compResult(lhs: List[List[Any]], rhs: List[List[Any]]): Boolean = {
    def toDouble(x: Any): Double = x match {
      case d: Double => d
      case d: Float => d.toDouble
      case d: java.math.BigDecimal => d.doubleValue()
      case d: BigDecimal => d.bigDecimal.doubleValue()
      case d: Number => d.doubleValue()
      case d: java.math.BigInteger => d.doubleValue()
    }

    def toInteger(x: Any): Long = x match {
      case d: Long => d
      case d: Integer => d.toLong
      case d: Short => d.toLong
      case d: java.math.BigInteger => d.longValue()
      case d: BigInt => d.bigInteger.longValue()
      case d: Number => d.longValue()
      case d: java.math.BigDecimal => d.longValue()
      case d: BigDecimal => d.bigDecimal.longValue()
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
          case (value, i) => rhs.length <= i || !compValue(value, rhs(i))
        }
      }
    }

    !lhs.zipWithIndex.exists {
      case (row, i) => rhs.length <= i || !compRow(row, rhs(i))
    }
  }

  def printResult(rowList: List[List[Any]]): Unit = {
    rowList.foreach{
      row => {
        row.foreach{
          value => print(value + " ")
        }
        println("")
      }
    }
  }
}
