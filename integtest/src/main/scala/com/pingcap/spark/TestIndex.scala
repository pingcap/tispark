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

import java.util.Properties

import com.google.common.collect.ImmutableSet

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by birdstorm on 2017/11/5.
  */
class TestIndex(prop: Properties) extends TestCase(prop) {

  private var colList: List[String] = _

  override protected val compareOpList = List("=", "<", ">", "<=", ">=")
  override protected val arithmeticOpList = List()

  // TODO: Eliminate these bugs
  private final val colSkipSet: ImmutableSet[String] =
    ImmutableSet.builder()
      //      .add("tp_datetime") // time zone shift
      .add("tp_year") // year in spark shows extra month and day
      //      .add("tp_time") // Time format is not the same in TiDB and spark
      //      .add("tp_binary")
      //      .add("tp_blob")
      .build()

  private val colSet: mutable.Set[String] = mutable.Set()

  private def testIndex(): Unit = {
    var result = false
    result |= execBothAndJudge("select * from test_index where a < 30")

    result |= execBothAndJudge("select * from test_index where d > \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d < \'116.5\'")
    result |= execBothAndJudge("select * from test_index where d > \'116.3\' and d < \'116.7\'")

    result |= execBothAndJudge("select * from test_index where d = \'116.72873\'")
    result |= execBothAndJudge("select * from test_index where d = \'116.72874\' and e < \'40.0452\'")

    result |= execBothAndJudge("select * from test_index where c > \'2008-02-06 14:03:58\'")
    result |= execBothAndJudge("select * from test_index where c >= \'2008-02-06 14:03:58\'")
    result |= execBothAndJudge("select * from test_index where c < \'2008-02-06 14:03:58\'")
    result |= execBothAndJudge("select * from test_index where c <= \'2008-02-06 14:03:58\'")
    result |= execBothAndJudge("select * from test_index where c = \'2008-02-06 14:03:58\'")

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

  def testFullDataTable(list: List[String]): Unit = {
    var result = false
    val startTime = System.currentTimeMillis()
    var count = 0
    for (sql <- list) {
      try {
        count += 1
        execAllAndJudge(sql)
        logger.info("Running num: " + count + " sql took " + (System.currentTimeMillis() - startTime) / 1000 + "s")
      } catch {
        case _: Throwable => logger.error("result: Run SQL " + sql + " Failed!")
      }
    }
    result = !result
    logger.warn(s"Result: Total Index test run: $inlineSQLNumber of ${list.size}")
    logger.warn(s"Result: Test ignored count:$testsSkipped, failed count:$testsFailed")
  }

  override def run(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    spark_jdbc.init(dbName)
    spark.init(dbName)
    jdbc.init(dbName)
    colList = jdbc.getTableColumnNames("full_data_type_table")
    prepareTestCol()
    testIndex()
    testFullDataTable(
        createArithmeticTest ++
        createPlaceHolderTest ++
        createInTest ++
        createBetween
//        createAggregate
    )
  }

  def prepareTestCol(): Unit = {
    colList.foreach(colSet.add)
    colSkipSet.foreach(colSet.remove)
  }


  def createLogicalAndOr(): List[String] = {
    createLogical("and") ::: createLogical("or")
  }

  private def createLogical(op: String): List[String] = {
    colSet.flatMap((lCol: String) =>
      colSet.map((rCol: String) =>
        select(lCol, rCol) + where(
          binaryOpWithName(
            binaryOpWithName(lCol, rCol, "=", withTbName = false),
            binaryOpWithName(lCol, "0", ">", withTbName = false),
            op,
            withTbName = false
          ))
      )).toList
  }

  // ***********************************************************************************************
  // ******************************** Below is test cases generated ********************************

  def createBetween(): List[String] = List(
    select("tp_int") + where(binaryOpWithName("tp_int", "-1202333 and 601508558", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_bigint") + where(binaryOpWithName("tp_bigint", "-2902580959275580308 and 9223372036854775807", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_decimal") + where(binaryOpWithName("tp_decimal", "2 and 200", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_double") + where(binaryOpWithName("tp_double", "0.2054466 and 3.1415926", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_float") + where(binaryOpWithName("tp_double", "-313.1415926 and 30.9412022", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_datetime") + where(binaryOpWithName("tp_datetime", "'2043-11-28 00:00:00' and '2017-09-07 11:11:11'", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_date") + where(binaryOpWithName("tp_date", "'2017-11-02' and '2043-11-28'", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_timestamp") + where(binaryOpWithName("tp_timestamp", "815587200000 and 1511862599000", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_year") + where(binaryOpWithName("tp_year", "1993 and 2017", "between", withTbName = false) + orderBy(ID_COL)),
    select("tp_real") + where(binaryOpWithName("tp_real", "4.44 and 0.5194052764001038", "between", withTbName = false) + orderBy(ID_COL))
  )

  def createAggregate(): List[String] = colSet.map((str: String) => select(str) + groupBy(str) + orderBy(str)).toList

  def createInTest(): List[String] = List(
    select("tp_int") + where(binaryOpWithName("tp_int", "(2333, 601508558, 4294967296, 4294967295)", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_bigint") + where(binaryOpWithName("tp_bigint", "(122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_varchar") + where(binaryOpWithName("tp_varchar", "('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_decimal") + where(binaryOpWithName("tp_decimal", "(2, 3, 4)", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_double") + where(binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_float") + where(binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_datetime") + where(binaryOpWithName("tp_datetime", "('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_date") + where(binaryOpWithName("tp_date", "('2017-11-02', '2043-11-28 00:00:00')", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_timestamp") + where(binaryOpWithName("tp_timestamp", "('2017-11-02 16:48:01')", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_year") + where(binaryOpWithName("tp_year", "('2017')", "in", withTbName = false) + orderBy(ID_COL)),
    select("tp_real") + where(binaryOpWithName("tp_real", "(4.44,0.5194052764001038)", "in", withTbName = false) + orderBy(ID_COL))
  )

  /**
    * We create test for each type, each operator
    *
    * @return
    */
  def createArithmeticTest: List[String] = {
    var res = ArrayBuffer.empty[String]
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")
    for (op <- arithmeticOpList) {
      for (lCol <- colSet) {
        if (!skipLocalSet.contains(lCol)) {
          for (rCol <- ARITHMETIC_CONSTANT) {
            if (!colSkipSet.contains(rCol)) {
              res += select(arithmeticOp(lCol, rCol, op)) + orderBy(ID_COL) + limit(10)
            }
          }
        }
      }
    }

    res.toList
  }

  def createPlaceHolderTest: List[String] = {
    var res = ArrayBuffer.empty[String]
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")

    val arithmeticSkipSet = mutable.Set[String]()
    arithmeticSkipSet.add("int")
    arithmeticSkipSet.add("float")
    arithmeticSkipSet.add("decimal")
    arithmeticSkipSet.add("double")
    arithmeticSkipSet.add("real")
    arithmeticSkipSet.add("bit")
    arithmeticSkipSet.add(ID_COL)

    for (op <- compareOpList) {
      for (col <- colSet) {
        if (!skipLocalSet.contains(col))
          for (placeHolder <- PLACE_HOLDER) {
            if (!placeHolder.eq("'PingCAP'") || !arithmeticSkipSet.exists(col.contains(_))) {
              res += select() + where(binaryOpWithName(
                col,
                placeHolder,
                op,
                withTbName = false
              ))
            }
          }
      }
    }

    res.toList
  }

  // ***********************************************************************************************
  // ******************************** Below is SQL build helper ************************************


  def groupBy(cols: String*): String = {
    s" group by $cols ".replace("WrappedArray", "")
  }

  def countId(): String = {
    s" count(1) "
  }

  def select(cols: String*): String = {
    var colList = ""
    for (col <- cols) {
      colList += col + ","
    }

    if (colList.length > 0) {
      colList = colList.substring(0, colList.length - 1)
    } else {
      colList = "*"
    }

    s"select " +
      colList +
      s" from " +
      s"$TABLE_NAME "
  }

  def orderBy(cols: String*): String = {
    s" order by $cols ".replace("WrappedArray", "").replace("(", "").replace(")", "")
  }

  def where(condition: String): String = {
    " where " + condition
  }

  def binaryOpWithName(leftCol: String, rightCol: String, op: String, withTbName: Boolean = true): String = {
    if (withTbName) {
      ""
    } else {
      leftCol + " " + op + " " + rightCol
    }
  }

  def arithmeticOp(l: String, r: String, op: String): String = {
    l + " " + op + " " + r
  }

  def limit(num: Int = 20): String = {
    " limit " + num
  }

}
