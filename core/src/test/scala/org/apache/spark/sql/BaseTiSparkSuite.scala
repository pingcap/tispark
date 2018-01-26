package org.apache.spark.sql

import java.io.{File, PrintWriter}
import java.sql.{Date, Timestamp}
import java.text.SimpleDateFormat

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
      .executeQuery("select * from tispark_test." + tableName + " limit 1")
    val metaData = rs.getMetaData
    var resList = ArrayBuffer.empty[String]
    for (i <- 1 to metaData.getColumnCount) {
      resList += metaData.getColumnName(i)
    }
    resList.toList
  }

  protected val compareOpList = List("=", "<", ">", "<=", ">=", "!=", "<>")
  protected val arithmeticOpList = List("+", "-", "*", "/", "%")
  protected val LEFT_TB_NAME = "A"
  protected val RIGHT_TB_NAME = "B"
  protected val TABLE_NAME = "full_data_type_table"
  protected val LITERAL_NULL = "null"
  protected val SCALE_FACTOR: Integer = 4 * 4
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
    "2017",
    BigDecimal.apply(2147868.65536).toString() // Decimal value
  )
  protected val PLACE_HOLDER: List[String] = List[String](
    LITERAL_NULL, // Null
    "'PingCAP'", // a simple test string
    "'2017-11-02'",
    "'2017-10-30'",
    "'2017-09-07 11:11:11'",
    "'2017-11-02 08:47:43'",
    "'fYfSp'"
  ) ++ ARITHMETIC_CONSTANT

  private var colList: List[String] = _

  // TODO: Eliminate these bugs
  private final val colSkipSet: Set[String] = Set("tp_bit", "tp_enum", "tp_set", "tp_time")

  private val colSet: mutable.Set[String] = mutable.Set()

  def run(): List[(String, List[String])] = {
    prepareTestCol()

    val tmp = mutable.ListBuffer[(String, List[String])]()
    tmp += Tuple2("createSimpleSelect", createSimpleSelect())
    tmp += Tuple2("createFirstLast", createFirstLast())
    tmp += Tuple2("createUnion", createUnion())
    tmp += Tuple2("createAggregate", createAggregate())
    tmp += Tuple2("createHaving", createHaving())
    tmp += Tuple2("createArithmeticAgg", createArithmeticAgg())
    tmp += Tuple2("createCartesianTypeTestCases", createCartesianTypeTestCases)
    tmp += Tuple2("createArithmeticTest", createArithmeticTest)
    tmp += Tuple2("createPlaceHolderTest", createPlaceHolderTest)
    tmp += Tuple2("createSimpleSelect", createSimpleSelect())
    tmp += Tuple2("createCount", createCount())
    tmp += Tuple2("createInTest", createInTest())
    tmp += Tuple2("createDistinct", createDistinct())
    tmp += Tuple2("createBetween", createBetween())

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

  def createLogicalAndOr(): List[String] = {
    createLogical("and") ::: createLogical("or")
  }

  private def createLogical(op: String): List[String] = {
    colSet
      .flatMap(
        (lCol: String) =>
          colSet.map(
            (rCol: String) =>
              select(lCol, rCol) + where(
                binaryOpWithName(
                  binaryOpWithName(lCol, rCol, "=", withTbName = false),
                  binaryOpWithName(lCol, "0", ">", withTbName = false),
                  op,
                  withTbName = false
                )
              )
          )
      )
      .toList
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

  def checkIgnore(value: Any, str: String): Boolean = {
    if (value == null) {
      false
    } else {
      value.toString.contains(str)
    }
  }

  def checkSparkJDBCIgnore(sparkJDBC: List[List[Any]]): Boolean = {
    sparkJDBC.exists(
      (row: List[Any]) =>
        row.exists(
          (str: Any) =>
            SparkIgnore.exists(
              (i: String) => checkIgnore(str, i)
            )
        )
    )
  }

  def testBundle(list: List[String]): Unit = {
    println("Size of test cases:" + list.size)
  }

  // ***********************************************************************************************
  // ******************************** Below is test cases generated ********************************

  def createUnion(): List[String] = {
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")
    skipLocalSet.add("tp_mediumtext")
    skipLocalSet.add("tp_longtext")

    colSet
      .diff(skipLocalSet)
      .map(
        (col: String) =>
          s"(select $col from $TABLE_NAME where $col < 0) union (select $col from $TABLE_NAME where $col > 0) order by $col"
      )
      .toList
  }

  def createHaving(): List[String] = List(
    s"select tp_int%1000 a, count(*) from $TABLE_NAME group by (tp_int%1000) having sum(tp_int%1000) > 100 order by a",
    s"select tp_bigint%1000 a, count(*) from $TABLE_NAME group by (tp_bigint%1000) having sum(tp_bigint%1000) < 100 order by a"
  )

  def createSimpleSelect(): List[String] =
    colSet.map((col: String) => select(col) + orderBy(col) + limit()).toList

  def createArithmeticAgg(): List[String] =
    colSet
      .flatMap(
        (col: String) =>
          Seq(
            s"select sum($col) from $TABLE_NAME",
            s"select avg($col) from $TABLE_NAME",
            s"select min($col) from $TABLE_NAME",
            s"select abs($col) from $TABLE_NAME",
            s"select max($col) from $TABLE_NAME"
          )
      )
      .toList

  def createFirstLast(): List[String] =
    colSet
      .flatMap(
        (col: String) =>
          Seq(
            s"select first($col) from $TABLE_NAME ${groupBy("tp_nvarchar")} ${orderBy("tp_nvarchar")}",
            s"select last($col) from $TABLE_NAME ${groupBy("tp_nvarchar")} ${orderBy("tp_nvarchar")}"
          )
      )
      .toList

  def createBetween(): List[String] = List(
    select("tp_int") + where(
      binaryOpWithName("tp_int", "-1202333 and 601508558", "between", withTbName = false)
    ),
    select("tp_bigint") + where(
      binaryOpWithName(
        "tp_bigint",
        "-2902580959275580308 and 9223372036854775807",
        "between",
        withTbName = false
      )
    ),
    select("tp_decimal") + where(
      binaryOpWithName("tp_decimal", "2 and 200", "between", withTbName = false)
    ),
    select("tp_double") + where(
      binaryOpWithName("tp_double", "0.2054466 and 3.1415926", "between", withTbName = false)
    ),
    select("tp_float") + where(
      binaryOpWithName("tp_double", "-313.1415926 and 30.9412022", "between", withTbName = false)
    ),
    select("tp_datetime") + where(
      binaryOpWithName(
        "tp_datetime",
        "'2043-11-28 00:00:00' and '2017-09-07 11:11:11'",
        "between",
        withTbName = false
      )
    ),
    select("tp_date") + where(
      binaryOpWithName("tp_date", "'2017-11-02' and '2043-11-28'", "between", withTbName = false)
    ),
    select("tp_timestamp") + where(
      binaryOpWithName(
        "tp_timestamp",
        "815587200000 and 1511862599000",
        "between",
        withTbName = false
      )
    ),
    //    select("tp_year") + where(binaryOpWithName("tp_year", "1993 and 2017", "between", withTbName = false)),
    select("tp_real") + where(
      binaryOpWithName("tp_real", "4.44 and 0.5194052764001038", "between", withTbName = false)
    )
  )

  def createCount(): List[String] = {
    colSet.map((col: String) => select(s"count($col)")).toList
  }

  def issueList(): List[String] = List(
    "select a.id_dt from full_data_type_table a where a.id_dt not in (select id_dt from full_data_type_table  where tp_decimal <> 1E10)",
    "select a.id_dt from full_data_type_table a left outer join (select id_dt from full_data_type_table  where tp_decimal <> 1E10) b on a.id_dt = b.id_dt where b.id_dt is null"
  )

  def createAggregate(): List[String] =
    colSet.map((str: String) => select(str) + groupBy(str) + orderBy(str)).toList

  def createInTest(): List[String] = List(
    select("tp_int") + where(
      binaryOpWithName(
        "tp_int",
        "(2333, 601508558, 4294967296, 4294967295)",
        "in",
        withTbName = false
      )
    ),
    select("tp_bigint") + where(
      binaryOpWithName(
        "tp_bigint",
        "(122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)",
        "in",
        withTbName = false
      )
    ),
    select("tp_varchar") + where(
      binaryOpWithName(
        "tp_varchar",
        "('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')",
        "in",
        withTbName = false
      )
    ),
    select("tp_decimal") + where(
      binaryOpWithName("tp_decimal", "(2, 3, 4)", "in", withTbName = false)
    ),
    select("tp_double") + where(
      binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in", withTbName = false)
    ),
    select("tp_float") + where(
      binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in", withTbName = false)
    ),
    select("tp_datetime") + where(
      binaryOpWithName(
        "tp_datetime",
        "('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')",
        "in",
        withTbName = false
      )
    ),
    select("tp_date") + where(
      binaryOpWithName("tp_date", "('2017-11-02', '2043-11-28 00:00:00')", "in", withTbName = false)
    ),
    select("tp_timestamp") + where(
      binaryOpWithName("tp_timestamp", "('2017-11-02 16:48:01')", "in", withTbName = false)
    ),
    //    select("tp_year") + where(binaryOpWithName("tp_year", "('2017')", "in", withTbName = false)),
    select("tp_real") + where(
      binaryOpWithName("tp_real", "(4.44,0.5194052764001038)", "in", withTbName = false)
    ),
    select("tp_longtext") + where(
      binaryOpWithName(
        "tp_longtext",
        "('很长的一段文字', 'OntPHB22qwSxriGUQ9RLfoiRkEMfEYFZdnAkL7SdpfD59MfmUXpKUAXiJpegn6dcMyfRyBhNw9efQfrl2yMmtM0zJx3ScAgTIA8djNnmCnMVzHgPWVYfHRnl8zENOD5SbrI4HAazss9xBVpikAgxdXKvlxmhfNoYIK0YYnO84MXKkMUinjPQ7zWHbh5lImp7g9HpIXgtkFFTXVvCaTr8mQXXOl957dxePeUvPv28GUdnzXTzk7thTbsWAtqU7YaK4QC4z9qHpbt5ex9ck8uHz2RoptFw71RIoKGiPsBD9YwXAS19goDM2H0yzVtDNJ6ls6jzXrGlJ6gIRG73Er0tVyourPdM42a5oDihfVP6XxjOjS0cmVIIppDSZIofkRfRhQWAunheFbEEPSHx3eybQ6pSIFd34Natgr2erFjyxFIRr7J535HT9aIReYIlocKK2ZI9sfcwhX0PeDNohY2tvHbsrHE0MlKCyVSTjPxszvFjCPlyqwQy')",
        "in",
        withTbName = false
      )
    ),
    select("tp_text") + where(
      binaryOpWithName(
        "tp_text",
        "('一般的文字', 'dQWD3XwSTevpbP5hADFdNO0dQvaueFhnGcJAm045mGv5fXttso')",
        "in",
        withTbName = false
      )
    )
    //    select("tp_bit") + where(binaryOpWithName("tp_bit", "(1)", "in", withTbName = false))
    //    select("tp_enum") + where(binaryOpWithName("tp_enum", "(1)", "in", withTbName = false)),
    //    select("tp_set") + where(binaryOpWithName("tp_set", "('a,b')", "in", withTbName = false))
  )

  def createDistinct(): List[String] = {
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_mediumtext")
    skipLocalSet.add("tp_longtext")
    skipLocalSet.add("tp_tinytext")
    skipLocalSet.add("tp_text")

    colSet.diff(skipLocalSet).map((str: String) => select(distinct(str)) + orderBy(str)).toList
  }

  /**
    * We create test for each type, each operator
    *
    * @return
    */
  def createSymmetryTypeTestCases: List[String] = {
    compareOpList.flatMap((op: String) => {
      colSet
        .map(
          (tp: String) =>
            select(tp, tp) + where(binaryOpWithName(tp, tp, op, withTbName = false)) + limit()
        )
        .toList
    })
  }

  def createSelfJoinTypeTest: List[String] = {
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")
    skipLocalSet.add("tp_mediumtext")
    skipLocalSet.add("tp_longtext")

    compareOpList.flatMap(
      (op: String) =>
        colSet
          .diff(skipLocalSet)
          .flatMap(
            (lCol: String) =>
              colSet
                .diff(skipLocalSet)
                .map((rCol: String) => buildBinarySelfJoinQuery(lCol, rCol, op))
          )
    )
  }

  def createCartesianTypeTestCases: List[String] = {
    val skipLocalSet = mutable.Set[String]()
    skipLocalSet.add("tp_nvarchar")
    skipLocalSet.add("tp_varchar")
    skipLocalSet.add("tp_char")
    skipLocalSet.add("tp_mediumtext")
    skipLocalSet.add("tp_longtext")

    compareOpList.flatMap(
      (op: String) =>
        colSet.flatMap(
          (lCol: String) =>
            colSet
              .filter(
                (rCol: String) =>
                  lCol.eq(rCol) || (!skipLocalSet.contains(lCol) && !skipLocalSet.contains(rCol))
              )
              .map(
                (rCol: String) =>
                  select(lCol, rCol) + where(binaryOpWithName(lCol, rCol, op, withTbName = false)) + orderBy(
                    ID_COL
                  ) + limit()
              )
        )
    )
  }

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
    //    skipLocalSet.add("tp_year")

    val arithmeticSkipSet = mutable.Set[String]()
    arithmeticSkipSet.add("tp_int")
    arithmeticSkipSet.add("tp_float")
    arithmeticSkipSet.add("tp_decimal")
    arithmeticSkipSet.add("tp_double")
    arithmeticSkipSet.add("tp_real")
    arithmeticSkipSet.add("tp_bit")
    arithmeticSkipSet.add(ID_COL)

    for (op <- compareOpList) {
      for (col <- colSet) {
        if (!skipLocalSet.contains(col))
          for (placeHolder <- PLACE_HOLDER) {
            if (!placeHolder.eq("'PingCAP'") || !arithmeticSkipSet.exists(col.contains(_))) {
              res += select(countId()) + where(
                binaryOpWithName(
                  col,
                  placeHolder,
                  op,
                  withTbName = false
                )
              )
            }
          }
      }
    }

    res.toList
  }

  // ***********************************************************************************************
  // ******************************** Below is SQL build helper ************************************

  def distinct(cols: String*): String = {
    s" distinct$cols ".replace("WrappedArray", "")
  }

  def groupBy(cols: String*): String = {
    s" group by $cols ".replace("WrappedArray", "")
  }

  def buildBinarySelfJoinQuery(lCol: String, rCol: String, op: String): String = {
    selfJoinSelect(
      Array(
        tableColDot(LEFT_TB_NAME, ID_COL),
        tableColDot(LEFT_TB_NAME, lCol),
        tableColDot(RIGHT_TB_NAME, rCol)
      ): _*
    ) +
      where(binaryOpWithName(lCol, rCol, op)) +
      orderBy(
        Array(
          tableColDot(LEFT_TB_NAME, ID_COL),
          tableColDot(RIGHT_TB_NAME, ID_COL)
        ): _*
      )
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

  def orderBy(cols: String*): String = {
    s" order by $cols ".replace("WrappedArray", "").replace("(", "").replace(")", "")
  }

  def where(condition: String): String = {
    " where " + condition
  }

  def binaryOpWithName(leftCol: String,
                       rightCol: String,
                       op: String,
                       withTbName: Boolean = true): String = {
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

  def createOrReplaceTempView(dbName: String, viewName: String, postfix: String = "_j"): Unit =
    spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"$dbName.$viewName")
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
      .createOrReplaceTempView(s"$viewName$postfix")

  def loadTestData(): Unit = {
    ti.tidbMapDatabase("tispark_test")
    createOrReplaceTempView("tispark_test", "full_data_type_table")
    colList = getTableColumnNames("full_data_type_table")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    loadTestData()
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

  test("select tp_int from full_data_type_table") {
//    val sqls = run()
//    var analyseFailed: Int = 0
//    var analyseSucceed = 0
//    val res = mutable.ArrayBuffer[String]()
//    sqls.foreach((tuple: (String, List[String])) => {
//      val clzName = tuple._1.replace("create", "") + "Suite"
//      val writer = new PrintWriter(new File(s"/home/novemser/testCases/$clzName.scala"))
//      writer.write(
//        s"""/*
//           | *
//           | * Copyright 2017 PingCAP, Inc.
//           | *
//           | * Licensed under the Apache License, Version 2.0 (the "License");
//           | * you may not use this file except in compliance with the License.
//           | * You may obtain a copy of the License at
//           | *
//           | *      http://www.apache.org/licenses/LICENSE-2.0
//           | *
//           | * Unless required by applicable law or agreed to in writing, software
//           | * distributed under the License is distributed on an "AS IS" BASIS,
//           | * See the License for the specific language governing permissions and
//           | * limitations under the License.
//           | *
//           | */
//           |
//           |package org.apache.spark.sql.expression
//           |
//           |import org.apache.spark.sql.BaseTiSparkSuite
//           |import org.apache.spark.sql.test.SharedSQLContext
//           |
//           |class $clzName
//           |  extends BaseTiSparkSuite
//           |  with SharedSQLContext {
//           """.stripMargin)
//      tuple._2.foreach((str: String) => {
//        try {
//          val jdbcQuery = str.replace("full_data_type_table", "full_data_type_table_j")
//          sql(jdbcQuery).show(1)
//          try {
//            sql(str)
//          } catch {
//            case e: AnalysisException if e.message.contains("due to data type mismatch") => throw e
//          }
//          analyseSucceed += 1
//          writer.write("\n")
//          writer.write(
//            s"""
//               |  test("$str") {
//               |    runTest("$str",
//               |            "$jdbcQuery")
//               |  }
//           """.stripMargin)
//        } catch {
//          case _: AnalysisException =>
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
