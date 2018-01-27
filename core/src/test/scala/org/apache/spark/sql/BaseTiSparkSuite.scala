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

import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BinaryType, StructField}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class BaseTiSparkSuite extends QueryTest with SharedSQLContext {

  private val eps = 1.0e-2

  private def toOutput(value: Any, colType: String): Any = value match {
    case _: Array[Byte] =>
      var str: String = new String
      for (b <- value.asInstanceOf[Array[Byte]]) {
        str = str.concat(b.toString)
      }
      str
    case _: BigDecimal =>
      value.asInstanceOf[BigDecimal].setScale(2, BigDecimal.RoundingMode.HALF_UP)
    case _: Date if colType.equalsIgnoreCase("YEAR") =>
      value.toString.split("-")(0)
    case default =>
      default
  }

  private def dfData(df: DataFrame, schema: scala.Array[StructField]): List[List[Any]] =
    df.collect()
      .map(row => {
        val rowRes = ArrayBuffer.empty[Any]
        for (i <- 0 until row.length) {
          if (row.get(i) == null) {
            rowRes += null
          } else if (schema(i).dataType.isInstanceOf[BinaryType]) {
            rowRes += new String(row.get(i).asInstanceOf[Array[Byte]])
          } else {
            rowRes += toOutput(row.get(i), schema(i).dataType.typeName)
          }
        }
        rowRes.toList
      })
      .toList

  protected def querySpark(query: String): List[List[Any]] = {
    val df = sql(query)
    val schema = df.schema.fields

    dfData(df, schema)
  }

  def queryTiDB(query: String): List[List[Any]] = {
    logger.info("Running query on TiDB: " + query)
    val statement = tidbConn.createStatement()
    val resultSet = statement.executeQuery(query)
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

  /**
   * Compare whether lhs equals to rhs
   *
   * @param isOrdered whether the input data `lhs` and `rhs` are sorted
   * @return true if results are the same
   */
  protected def compResult(lhs: List[List[Any]],
                           rhs: List[List[Any]],
                           isOrdered: Boolean = true): Boolean = {
    def toDouble(x: Any): Double = x match {
      case d: Double               => d
      case d: Float                => d.toDouble
      case d: java.math.BigDecimal => d.doubleValue()
      case d: BigDecimal           => d.bigDecimal.doubleValue()
      case d: Number               => d.doubleValue()
      case _                       => 0.0
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
      } else
        lhs match {
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

    def comp(lhs: List[List[Any]], rhs: List[List[Any]]): Boolean = {
      !lhs.zipWithIndex.exists {
        case (row, i) => !compRow(row, rhs(i))
      }
    }

    try {
      if (!isOrdered) {
        comp(
          lhs.sortWith((_1, _2) => _1.mkString("").compare(_2.mkString("")) < 0),
          rhs.sortWith((_1, _2) => _1.mkString("").compare(_2.mkString("")) < 0)
        )
      } else {
        comp(lhs, rhs)
      }
    } catch {
      // TODO:Remove this temporary exception handling
      //      case _:RuntimeException => false
      case _: Throwable => false
    }
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

  private def createGeneralTest(): List[String] = {
    val result = mutable.ArrayBuffer[String]()
    result += "select * from test_index where a < 30"
    result += "select * from test_index where d > \'116.5\'"
    result += "select * from test_index where d < \'116.5\'"
    result += "select * from test_index where d > \'116.3\' and d < \'116.7\'"
    result += "select * from test_index where d = \'116.72873\'"
    result += "select * from test_index where d = \'116.72874\' and e < \'40.0452\'"
    result += "select * from test_index where c > \'2008-02-06 14:03:58\'"
    result += "select * from test_index where c >= \'2008-02-06 14:03:58\'"
    result += "select * from test_index where c < \'2008-02-06 14:03:58\'"
    result += "select * from test_index where c <= \'2008-02-06 14:03:58\'"
    result += "select * from test_index where c = \'2008-02-06 14:03:58\'"
    result += "select * from test_index where c > date \'2008-02-05\'"
    result += "select * from test_index where c >= date \'2008-02-05\'"
    result += "select * from test_index where c < date \'2008-02-05\'"
    result += "select * from test_index where c <= date \'2008-02-05\'"
    result += "select * from test_index where DATE(c) = date \'2008-02-05\'"
    result += "select * from test_index where DATE(c) > date \'2008-02-05\'"
    result += "select * from test_index where DATE(c) >= date \'2008-02-05\'"
    result += "select * from test_index where DATE(c) < date \'2008-02-05\'"
    result += "select * from test_index where DATE(c) <= date \'2008-02-05\'"
    result += "select * from test_index where c <> date \'2008-02-05\'"
    result += "select * from test_index where c > \'2008-02-04 14:00:00\' and d > \'116.5\'"
    result += "select * from test_index where d = \'116.72873\' and c > \'2008-02-04 14:00:00\'"
    result += "select * from test_index where d = \'116.72873\' and c < \'2008-02-04 14:00:00\'"
    result.toList
  }

  def createOrReplaceTempView(dbName: String, viewName: String, postfix: String = "_j"): Unit =
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$dbName.$viewName")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
      .createOrReplaceTempView(s"$viewName$postfix")

  def loadTestData(): Unit = {
    loadTiSparkTestData()
    tidbConn.setCatalog("tispark_test")
    ti.tidbMapDatabase("tispark_test")
    createOrReplaceTempView("tispark_test", "full_data_type_table")
    createOrReplaceTempView("tispark_test", "full_data_type_table_idx")
  }

  /**
   * Make sure to load test before running tests.
   */
  def loadTiSparkTestData(): Unit = {
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    loadTestData()
  }

  def setLogLevel(level: String): Unit = {
    spark.sparkContext.setLogLevel(level)
  }

  def runTest(qSpark: String, qJDBC: String): Unit = {
    var r1: List[List[Any]] = null
    var r2: List[List[Any]] = null
    var r3: List[List[Any]] = null

    try {
      r1 = querySpark(qSpark)
    } catch {
      case e: Throwable => fail(e)
    }

    try {
      r2 = querySpark(qJDBC)
    } catch {
      case _: Throwable => // JDBC failed
    }

    val isOrdered = qSpark.contains(" order by ")

    if (!compResult(r1, r2, isOrdered)) {
      r3 = queryTiDB(qSpark)
      if (!compResult(r1, r3, isOrdered)) {
        fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2\nTiDB:\t\t$r3")
      }
    }
  }
}
