package com.pingcap.spark

import com.google.common.collect.ImmutableSet

import scala.collection.mutable.ArrayBuffer

class DAGTestCase(colList: List[String]) {
  val compareOpList = List("=", "<", ">", "<=", ">=", "!=", "<>")
  val LEFT_TB_NAME = "A"
  val RIGHT_TB_NAME = "B"
  val TABLE_NAME = "full_data_type_table"

  // TODO: Eliminate these bugs
  private final val colSkipSet: ImmutableSet[String] =
    ImmutableSet.builder()
      .add("tp_bit") // bit cannot be push down
      .add("tp_datetime") // time zone shift
      .add("tp_year") // year in spark shows extra month and day
      .add("tp_time") // Time format is not the same in TiDB and spark
      .build()

  /**
    * We create test for each type, each operator
    *
    * @return
    */
  def createTypeTestCases: List[String] = {
    var res = ArrayBuffer.empty[String]
    for (op <- compareOpList) {
      for (tp <- colList) {
        if (!colSkipSet.contains(tp)) {
          res += selfJoinSelect(
            List(
              tableColDot(LEFT_TB_NAME, tp),
              tableColDot(RIGHT_TB_NAME, tp)
            )
          ) +
            where(binaryOpWithName(tp, tp, op)) +
            orderBy(tableColDot(LEFT_TB_NAME, "id_dt"))
        }
      }
    }

    res.toList
  }

  def selfJoinSelect(cols: List[String]): String = {
    var colList = ""
    for (col <- cols) {
      colList += col + ","
    }

    if (colList.length > 0) {
      colList = colList.substring(0, colList.length - 1)
    }

    s"select " +
      colList +
      s" from " +
      s"$TABLE_NAME $LEFT_TB_NAME join full_data_type_table $RIGHT_TB_NAME " +
      s"on $LEFT_TB_NAME.id_dt = $RIGHT_TB_NAME.id_dt"
  }

  def orderBy(condition: String): String = {
    " order by " + condition
  }

  def where(condition: String): String = {
    " where " + condition
  }

  def binaryOpWithName(leftCol: String, rightCol: String, op: String): String = {
    tableColDot(LEFT_TB_NAME, leftCol) + " " + op + " " + tableColDot(RIGHT_TB_NAME, rightCol)
  }

  def tableColDot(table: String, col: String): String = {
    table + dot + col
  }

  def dot() = "."

  def limit(num: Int): String = {
    "limit " + num
  }
}

object DAGTestCase {
  def main(args: Array[String]): Unit = {
    for (str <- new DAGTestCase(List("tp1", "tp2", "tp3")).createTypeTestCases) {
      println(str)
    }
  }
}