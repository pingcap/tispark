package com.pingcap.spark

import java.util.Properties
import java.util.stream.Collector

import com.google.common.collect.ImmutableSet
import com.typesafe.scalalogging.slf4j.Logger

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

class DAGTestCase(colList: List[String], prop: Properties) extends TestCase(prop) {
  private val compareOpList = List("=", "<", ">", "<=", ">=", "!=", "<>")
  private val arithmeticOpList = List("+", "-", "*", "/", "%")
  private val LEFT_TB_NAME = "A"
  private val RIGHT_TB_NAME = "B"
  private val TABLE_NAME = "full_data_type_table"
  private val LITERAL_NULL = "null"
  private val SCALE_FACTOR = 4 * 4
  private val ID_COL = "id_dt"
  private val ARITHMETIC_CONSTANT = List[String](
    java.lang.Long.MAX_VALUE.toString,
    java.lang.Long.MIN_VALUE.toString,
    java.lang.Double.MAX_VALUE.toString,
    java.lang.Double.MIN_VALUE.toString,
    java.lang.Integer.MAX_VALUE.toString,
    java.lang.Integer.MIN_VALUE.toString,
    java.lang.Short.MAX_VALUE.toString,
    java.lang.Short.MIN_VALUE.toString,
    java.lang.Byte.MAX_VALUE.toString,
    java.lang.Byte.MIN_VALUE.toString,
    "0",
    BigDecimal.apply(2147868.65536).toString() // Decimal value
  )
  private val PLACE_HOLDER = List[String](
    //    LITERAL_NULL, // Null
    "'PingCAP'" // a simple test string
  ) ++ ARITHMETIC_CONSTANT

  // TODO: Eliminate these bugs
  private final val colSkipSet: ImmutableSet[String] =
    ImmutableSet.builder()
      .add("tp_bit") // bit cannot be push down
      .add("tp_datetime") // time zone shift
      .add("tp_year") // year in spark shows extra month and day
      .add("tp_time") // Time format is not the same in TiDB and spark
      .add("tp_enum")
      .add("tp_set")
      .add("tp_binary")
      .add("tp_blob")
      .build()

  private val colSet: mutable.Set[String] = mutable.Set()

  override def run(dbName: String): Unit = {
    prepareTestCol()

    testBundle(
      createSymmetryTypeTestCases ++
        createCartesianTypeTestCases ++
        createArithmeticTest ++
        createPlaceHolderTest
    )
  }

  def prepareTestCol(): Unit = {
    colList.foreach(colSet.add)
    colSkipSet.foreach(colSet.remove)
  }

  def createDistinct(): List[String] = {
    colSet.map((str: String) =>
      select(distinct(str))
    ).toList
  }

  def createLogicalAndOr(): List[String] = {
    createLogical("and") ::: createLogical("or")
  }

  private def createLogical(op:String): List[String] = {
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

  def createBit() = {

  }

  def distinct(cols: String*): String = {
    s" distinct$cols ".replace("WrappedArray", "")
  }

  def testBundle(list: List[String]): Unit = {
    var result = false

    for (sql <- list) {
      val exeRes = execBothAndJudge(sql)
      if (exeRes)
        logger.error("result: Test sql failed, " + sql)
      result |= exeRes
    }
    result = !result
    logger.info("result: Overall DAG test :" + result)
  }

  // ***********************************************************************************************
  // ******************************** Below is test cases generated ********************************

  /**
    * We create test for each type, each operator
    *
    * @return
    */
  def createSymmetryTypeTestCases: List[String] = {
    compareOpList.flatMap((op: String) => {
      colSet.map((tp: String) => buildBinarySelfJoinQuery(tp, tp, op) + limit()).toList
    })
  }

  def createCartesianTypeTestCases: List[String] = {
    compareOpList.flatMap((op: String) =>
      colSet.flatMap((lCol: String) =>
        colSet.map((rCol: String) =>
          buildBinarySelfJoinQuery(lCol, rCol, op)
        )
      )
    )
  }

  def createArithmeticTest: List[String] = {
    var res = ArrayBuffer.empty[String]
    for (op <- arithmeticOpList) {
      for (lCol <- colList) {
        for (rCol <- ARITHMETIC_CONSTANT) {
          if (!colSkipSet.contains(rCol)) {
            res += select(arithmeticOp(lCol, rCol, op)) + orderBy(ID_COL) + limit(10)
          }
        }
      }
    }

    res.toList
  }

  def createPlaceHolderTest: List[String] = {
    var res = ArrayBuffer.empty[String]
    for (op <- compareOpList) {
      for (col <- colSet) {
        for (placeHolder <- PLACE_HOLDER) {
          res += select(countId()) + where(binaryOpWithName(
            col,
            placeHolder,
            op,
            withTbName = false
          ))
        }
      }
    }

    res.toList
  }

  // ***********************************************************************************************
  // ******************************** Below is SQL build helper ************************************

  def buildBinarySelfJoinQuery(lCol: String, rCol: String, op: String): String = {
    selfJoinSelect(
      Array(
        tableColDot(LEFT_TB_NAME, lCol),
        tableColDot(RIGHT_TB_NAME, rCol)
      ): _*
    ) +
      where(binaryOpWithName(lCol, rCol, op)) +
      orderBy(tableColDot(LEFT_TB_NAME, ID_COL))
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
    }

    s"select " +
      colList +
      s" from " +
      s"$TABLE_NAME "
  }

  def selfJoinSelect(cols: String*): String = {
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
      s"on $LEFT_TB_NAME.id_dt > $RIGHT_TB_NAME.id_dt * $SCALE_FACTOR"
  }

  def orderBy(condition: String): String = {
    " order by " + condition
  }

  def where(condition: String): String = {
    " where " + condition
  }

  def binaryOpWithName(leftCol: String, rightCol: String, op: String, withTbName: Boolean = true): String = {
    if (withTbName) {
      tableColDot(LEFT_TB_NAME, leftCol) + " " + op + " " + tableColDot(RIGHT_TB_NAME, rightCol)
    } else {
      leftCol + " " + op + " " + rightCol
    }
  }

  def tableColDot(table: String, col: String): String = {
    table + dot + col
  }

  def dot() = "."

  def arithmeticOp(l: String, r: String, op: String): String = {
    l + " " + op + " " + r
  }

  def limit(num: Int = 20): String = {
    " limit " + num
  }
}

object DAGTestCase {
  def main(args: Array[String]): Unit = {
    val dAGTestCase = new DAGTestCase(List("tp1", "tp2", "tp3"), new Properties())
    dAGTestCase.prepareTestCol()
    for (str <- dAGTestCase.createLogicalAndOr) {
      println(str)
    }
  }
}