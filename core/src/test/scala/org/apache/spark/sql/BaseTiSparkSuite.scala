package org.apache.spark.sql

import java.io.{File, PrintWriter}
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
   *
   * @param lhs
   * @param rhs
   * @param isOrdered
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

  protected val compareOpList = List("=", "<", ">", "<=", ">=", "!=", "<>")
  protected val arithmeticOpList = List("+", "-", "*", "/", "%")
  protected val TABLE_NAME = "full_data_type_table_idx"
  protected val LITERAL_NULL = "null"
  protected val ID_COL = "id_dt"

  protected val ARITHMETIC_CONSTANT: List[String] = List[String](
    java.lang.Long.MAX_VALUE.toString,
    java.lang.Long.MIN_VALUE.toString,
    java.lang.Double.MAX_VALUE.toString,
    java.lang.Double.MIN_VALUE.toString,
    3.14159265358979D.toString,
    "2.34E10",
    java.lang.Integer.MAX_VALUE.toString,
    java.lang.Integer.MIN_VALUE.toString,
    java.lang.Short.MAX_VALUE.toString,
    java.lang.Short.MIN_VALUE.toString,
    java.lang.Byte.MAX_VALUE.toString,
    java.lang.Byte.MIN_VALUE.toString,
    "0",
    BigDecimal.apply(2147868.65536).toString() // Decimal value
  )
  protected val PLACE_HOLDER: List[String] = List[String](
    LITERAL_NULL, // Null
    "'PingCAP'", // a simple test string
    "'2017-11-02'"
  ) ++ ARITHMETIC_CONSTANT

  protected val DATE_DATA: List[String] = List[String](
    "date '2017-10-30'",
    "date '2017-11-02'"
  )

  protected val DATETIME_DATA: List[String] = List[String](
    "timestamp '2017-11-02 00:00:00'",
    "timestamp '2017-11-02 08:47:43'",
    "timestamp '2017-09-07 11:11:11'"
  )

  private var colList: List[String] = _

  // TODO: Eliminate these bugs
  private final val colSkipSet: Set[String] = Set("tp_bit", "tp_enum", "tp_set", "tp_time")

  private val colSet: mutable.Set[String] = mutable.Set()

  def run(): List[(String, List[String])] = {
    prepareTestCol()

    val tmp = mutable.ListBuffer[(String, List[String])]()
    tmp += Tuple2("createAggregate", createAggregate())
    tmp += Tuple2("createPlaceHolder", createPlaceHolderTest)
    tmp += Tuple2("createInTest", createInTest())
    tmp += Tuple2("createSpecial", createSpecial())
    tmp += Tuple2("createBetween", createBetween())
    tmp += Tuple2("createJoin", createJoin())
//    tmp += Tuple2("createGeneral", createGeneralTest())

    val res = mutable.ListBuffer[(String, List[String])]()
    tmp.foreach((tuple: (String, List[String])) => {
      val name = tuple._1
      tuple._2
        .grouped(1000)
        .zipWithIndex
        .foreach((tuple: (List[String], Int)) => {
          res += Tuple2(name + tuple._2, tuple._1)
        })
    })
    res.toList
  }

  def prepareTestCol(): Unit = {
    colList.foreach(colSet.add)
    colSkipSet.foreach(colSet.remove)
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

  private final val SparkIgnore = Set[String](
    "type mismatch",
    "only support precision",
    "Decimal scale (18) cannot be greater than precision ",
    "0E-11", // unresolvable precision fault
    "overflows"
    //    "unknown error Other"
    //    "Error converting access pointsnull"
  )

  def testBundle(list: List[String]): Unit = {
    println("Size of test cases:" + list.size)
  }

  // ***********************************************************************************************
  // ******************************** Below is test cases generated ********************************

  def createBetween(): List[String] = List(
    select("tp_int") + where(
      binaryOpWithName("tp_int", "-1202333 and 601508558", "between") + orderBy(ID_COL)
    ),
    select("tp_bigint") + where(
      binaryOpWithName("tp_bigint", "-2902580959275580308 and 9223372036854775807", "between") + orderBy(
        ID_COL
      )
    ),
    select("tp_decimal") + where(
      binaryOpWithName("tp_decimal", "2 and 200", "between") + orderBy(ID_COL)
    ),
    select("tp_double") + where(
      binaryOpWithName("tp_double", "0.2054466 and 3.1415926", "between") + orderBy(ID_COL)
    ),
    select("tp_float") + where(
      binaryOpWithName("tp_double", "-313.1415926 and 30.9412022", "between") + orderBy(ID_COL)
    ),
    select("tp_datetime") + where(
      binaryOpWithName("tp_datetime", "'2043-11-28 00:00:00' and '2017-09-07 11:11:11'", "between") + orderBy(
        ID_COL
      )
    ),
    select("tp_date") + where(
      binaryOpWithName("tp_date", "'2017-11-02' and '2043-11-28'", "between") + orderBy(ID_COL)
    ),
    select("tp_timestamp") + where(
      binaryOpWithName("tp_timestamp", "815587200000 and 1511862599000", "between") + orderBy(
        ID_COL
      )
    ),
    //    select("tp_year") + where(
    //      binaryOpWithName("tp_year", "1993 and 2017", "between") + orderBy(ID_COL)
    //    ),
    select("tp_real") + where(
      binaryOpWithName("tp_real", "4.44 and 0.5194052764001038", "between") + orderBy(ID_COL)
    )
  )

  def createAggregate(): List[String] =
    colSet.map((str: String) => select(str) + groupBy(str) + orderBy(str)).toList

  def createInTest(): List[String] = List(
    select("tp_int") + where(
      binaryOpWithName("tp_int", "(2333, 601508558, 4294967296, 4294967295)", "in") + orderBy(
        ID_COL
      )
    ),
    select("tp_bigint") + where(
      binaryOpWithName(
        "tp_bigint",
        "(122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)",
        "in"
      ) + orderBy(ID_COL)
    ),
    select("tp_varchar") + where(
      binaryOpWithName("tp_varchar", "('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')", "in") + orderBy(
        ID_COL
      )
    ),
    select("tp_decimal") + where(
      binaryOpWithName("tp_decimal", "(2, 3, 4)", "in") + orderBy(ID_COL)
    ),
    select("tp_double") + where(
      binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in") + orderBy(ID_COL)
    ),
    select("tp_float") + where(
      binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in") + orderBy(ID_COL)
    ),
    select("tp_datetime") + where(
      binaryOpWithName(
        "tp_datetime",
        "('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')",
        "in"
      ) + orderBy(ID_COL)
    ),
    select("tp_date") + where(
      binaryOpWithName("tp_date", "('2017-11-02', '2043-11-28 00:00:00')", "in") + orderBy(ID_COL)
    ),
    select("tp_timestamp") + where(
      binaryOpWithName("tp_timestamp", "('2017-11-02 16:48:01')", "in") + orderBy(ID_COL)
    ),
    //    select("tp_year") + where(binaryOpWithName("tp_year", "('2017')", "in") + orderBy(ID_COL)),
    select("tp_real") + where(
      binaryOpWithName("tp_real", "(4.44,0.5194052764001038)", "in") + orderBy(ID_COL)
    )
  )

  def createSpecial(): List[String] = {
    var list: List[String] = List.empty[String]
    for (op <- compareOpList) {
      for (date <- DATE_DATA) {
        list ++= List(s"select * from $TABLE_NAME where tp_date " + op + s" date($date)")
      }
    }

    for (op <- compareOpList) {
      for (datetime <- DATETIME_DATA) {
        list ++= List(
          s"select * from $TABLE_NAME where tp_datetime " + op + s" timestamp($datetime)"
        )
      }
    }

    for (op <- compareOpList) {
      for (timestamp <- DATETIME_DATA) {
        list ++= List(
          s"select * from $TABLE_NAME where tp_timestamp " + op + s" timestamp($timestamp)"
        )
      }
    }
    list
  }

  def createJoin(): List[String] = {
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")

    colSet
      .diff(skipLocalSet)
      .map(
        (col: String) =>
          s"select a.$ID_COL from $TABLE_NAME a join $TABLE_NAME b on a.$col = b.$col order by a.$col"
      )
      .toList
  }

  def createPlaceHolderTest: List[String] = {
    var res = ArrayBuffer.empty[String]
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")

    for (op <- compareOpList) {
      for (col <- colSet) {
        if (!skipLocalSet.contains(col))
          for (placeHolder <- PLACE_HOLDER) {
            res += select(col) + where(
              binaryOpWithName(
                col,
                placeHolder,
                op
              )
            )
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

  def binaryOpWithName(leftCol: String, rightCol: String, op: String): String = {
    leftCol + " " + op + " " + rightCol
  }

  def arithmeticOp(l: String, r: String, op: String): String = {
    l + " " + op + " " + r
  }

  def limit(num: Int = 20): String = {
    " limit " + num
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
    ti.tidbMapDatabase("tispark_test")
    createOrReplaceTempView("tispark_test", "full_data_type_table")
    createOrReplaceTempView("tispark_test", "full_data_type_table_idx")
    colList = getTableColumnNames("full_data_type_table")
  }

  /**
   * Make sure to load test befor running tests.
   */
  def loadTiSparkTestData(): Unit = {
    // Load index test data
    var queryString = resourceToString(
      s"tispark-test/IndexTest.sql",
      classLoader = Thread.currentThread().getContextClassLoader
    )
    tidbConn.createStatement().execute(queryString)
    logger.info("Loading IndexTest.sql successfully.")
    // Load expression test data
    queryString = resourceToString(
      s"tispark-test/TiSparkTest.sql",
      classLoader = Thread.currentThread().getContextClassLoader
    )
    tidbConn.createStatement().execute(queryString)
    logger.info("Loading TiSparkTest.sql successfully.")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    loadTestData()
    setLogLevel("WARN")
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

  /**
   * This is only a test generation tool, do not use it if you do not know what
   * this code will do!
   */
  test("GenRelation") {
//    cancel()
//    val sqls = run()
//    var analyseFailed: Int = 0
//    var analyseSucceed = 0
//    val res = mutable.ArrayBuffer[String]()
//    sqls.foreach((tuple: (String, List[String])) => {
//      val clzName = tuple._1.replace("create", "") + "Suite"
//      val writer = new PrintWriter(new File(s"/home/novemser/testCases/index/$clzName.scala"))
//      writer.write(s"""/*
//                      | *
//                      | * Copyright 2017 PingCAP, Inc.
//                      | *
//                      | * Licensed under the Apache License, Version 2.0 (the "License");
//                      | * you may not use this file except in compliance with the License.
//                      | * You may obtain a copy of the License at
//                      | *
//                      | *      http://www.apache.org/licenses/LICENSE-2.0
//                      | *
//                      | * Unless required by applicable law or agreed to in writing, software
//                      | * distributed under the License is distributed on an "AS IS" BASIS,
//                      | * See the License for the specific language governing permissions and
//                      | * limitations under the License.
//                      | *
//                      | */
//                      |
//                      |package org.apache.spark.sql.expression.index
//                      |
//                      |import org.apache.spark.sql.BaseTiSparkSuite
//                      |import org.apache.spark.sql.test.SharedSQLContext
//                      |
//                      |class $clzName
//                      |  extends BaseTiSparkSuite
//                      |  with SharedSQLContext {
//               """.stripMargin)
//      tuple._2.foreach((str: String) => {
//        try {
//          val jdbcQuery = str.replace(TABLE_NAME, s"${TABLE_NAME}_j")
//          sql(jdbcQuery).show(1)
//          try {
//            sql(str)
//          } catch {
//            case e: AnalysisException if e.message.contains("due to data type mismatch") => throw e
//          }
//          analyseSucceed += 1
//          writer.write("\n")
//          writer.write(s"""
//                          |  test("$str") {
//                          |    runTest("$str",
//                          |            "$jdbcQuery")
//                          |  }
//               """.stripMargin)
//        } catch {
//          case e: AnalysisException =>
//            analyseFailed += 1
//        }
//      })
//      writer.write("\n}")
//      writer.close()
//    })
//
//    println("Succeed:" + analyseSucceed + ", Failed:" + analyseFailed)
  }
}
