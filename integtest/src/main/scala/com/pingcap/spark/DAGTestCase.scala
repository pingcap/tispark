package com.pingcap.spark

import java.util.Properties

import com.google.common.collect.ImmutableSet

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class DAGTestCase(prop: Properties) extends TestCase(prop) {
  private val SPARK_DEFAULT_TO_IGNORE = Set[String](
    "select tp_datetime,tp_date from full_data_type_table  where tp_datetime = tp_date order by id_dt  limit 20", // we cannot get anything from spark, but can get some data from TiDB
    "select tp_date,tp_datetime from full_data_type_table  where tp_date = tp_datetime order by id_dt  limit 20",
    "select tp_date,tp_datetime from full_data_type_table  where tp_date < tp_datetime order by id_dt  limit 20"
  )
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
  private val PLACE_HOLDER = List[String](
    LITERAL_NULL, // Null
    "'PingCAP'", // a simple test string
    "'2043-11-28'",
    "'2017-09-07 11:11:11'"
  ) ++ ARITHMETIC_CONSTANT
  private var colList: List[String] = _

  // TODO: Eliminate these bugs
  private final val colSkipSet: ImmutableSet[String] =
    ImmutableSet.builder()
      .add("tp_bit") // TODO: bit type is ignored due to false decoding
      //      .add("tp_datetime") // time zone shift
      //      .add("tp_year") // year in spark shows extra month and day
      //      .add("tp_time") // Time format is not the same in TiDB and spark
      .add("tp_enum") // TODO: enum and set are ignored because we are not supporting them yet
      .add("tp_set")
//      .add("tp_binary")
//      .add("tp_blob")
      .build()

  private val colSet: mutable.Set[String] = mutable.Set()

  override def run(dbName: String, testCases: ArrayBuffer[(String, String)]): Unit = {
    spark_jdbc.init(dbName)
    spark.init(dbName)
    jdbc.init(dbName)
    colList = jdbc.getTableColumnNames("full_data_type_table")
    prepareTestCol()
    testBundle(
      //      createSelfJoinTypeTest ++
      //      createSymmetryTypeTestCases ++
      createCartesianTypeTestCases ++
      createArithmeticTest ++
      createPlaceHolderTest ++
      createInTest ++
      createDistinct ++
      createBetween ++
      createArithmeticAgg ++
      createFirstLast ++
      createUnion ++
      createAggregate ++
      createHaving
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


  def testBundle(list: List[String]): Unit = {
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
    logger.warn("Result: Total DAG test run:" + inlineSQLNumber + " of " + list.size)
    logger.warn(s"Result: Test ignored count:$testsSkipped, failed count:$testsFailed")
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

    colSet.diff(skipLocalSet).map((col: String) =>
      s"(select $col from $TABLE_NAME where $col < 0) union (select $col from $TABLE_NAME where $col > 0) order by $col").toList
  }

  def createHaving(): List[String] = List(
    s"select tp_int%1000 from $TABLE_NAME group by (tp_int%1000) having sum(tp_int%1000) > 100",
    s"select tp_bigint%1000 from $TABLE_NAME group by (tp_bigint%1000) having sum(tp_bigint%1000) < 100"
  )

  def createArithmeticAgg(): List[String] = colSet.map((col: String) => s"select sum($col),avg($col),min($col),max($col) from $TABLE_NAME group by $col ${orderBy(col)}").toList

  def createFirstLast(): List[String] = colSet.map((col: String) => s"select first($col), last($col) from $TABLE_NAME group by $col order by $col").toList

  def createBetween(): List[String] = List(
    select("tp_int") + where(binaryOpWithName("tp_int", "-1202333 and 601508558", "between", withTbName = false)),
    select("tp_bigint") + where(binaryOpWithName("tp_bigint", "-2902580959275580308 and 9223372036854775807", "between", withTbName = false)),
    select("tp_decimal") + where(binaryOpWithName("tp_decimal", "2 and 200", "between", withTbName = false)),
    select("tp_double") + where(binaryOpWithName("tp_double", "0.2054466 and 3.1415926", "between", withTbName = false)),
    select("tp_float") + where(binaryOpWithName("tp_double", "-313.1415926 and 30.9412022", "between", withTbName = false)),
    select("tp_datetime") + where(binaryOpWithName("tp_datetime", "'2043-11-28 00:00:00' and '2017-09-07 11:11:11'", "between", withTbName = false)),
    select("tp_date") + where(binaryOpWithName("tp_date", "'2017-11-02' and '2043-11-28'", "between", withTbName = false)),
    select("tp_timestamp") + where(binaryOpWithName("tp_timestamp", "815587200000 and 1511862599000", "between", withTbName = false)),
    select("tp_year") + where(binaryOpWithName("tp_year", "1993 and 2017", "between", withTbName = false)),
    select("tp_real") + where(binaryOpWithName("tp_real", "4.44 and 0.5194052764001038", "between", withTbName = false))
  )

  def issueList(): List[String] = List(
    "select a.id_dt from full_data_type_table a where a.id_dt not in (select id_dt from full_data_type_table  where tp_decimal <> 1E10)",
    "select a.id_dt from full_data_type_table a left outer join (select id_dt from full_data_type_table  where tp_decimal <> 1E10) b on a.id_dt = b.id_dt where b.id_dt is null"
  )

  def createAggregate(): List[String] = colSet.map((str: String) => select(str) + groupBy(str) + orderBy(str)).toList

  def createInTest(): List[String] = List(
    select("tp_int") + where(binaryOpWithName("tp_int", "(2333, 601508558, 4294967296, 4294967295)", "in", withTbName = false)),
    select("tp_bigint") + where(binaryOpWithName("tp_bigint", "(122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)", "in", withTbName = false)),
    select("tp_varchar") + where(binaryOpWithName("tp_varchar", "('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')", "in", withTbName = false)),
    select("tp_decimal") + where(binaryOpWithName("tp_decimal", "(2, 3, 4)", "in", withTbName = false)),
    select("tp_double") + where(binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in", withTbName = false)),
    select("tp_float") + where(binaryOpWithName("tp_double", "(0.2054466,3.1415926,0.9412022)", "in", withTbName = false)),
    select("tp_datetime") + where(binaryOpWithName("tp_datetime", "('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')", "in", withTbName = false)),
    select("tp_date") + where(binaryOpWithName("tp_date", "('2017-11-02', '2043-11-28 00:00:00')", "in", withTbName = false)),
    select("tp_timestamp") + where(binaryOpWithName("tp_timestamp", "('2017-11-02 16:48:01')", "in", withTbName = false)),
    select("tp_year") + where(binaryOpWithName("tp_year", "('2017')", "in", withTbName = false)),
    select("tp_real") + where(binaryOpWithName("tp_real", "(4.44,0.5194052764001038)", "in", withTbName = false)),
    select("tp_longtext") + where(binaryOpWithName("tp_longtext", "('很长的一段文字', 'OntPHB22qwSxriGUQ9RLfoiRkEMfEYFZdnAkL7SdpfD59MfmUXpKUAXiJpegn6dcMyfRyBhNw9efQfrl2yMmtM0zJx3ScAgTIA8djNnmCnMVzHgPWVYfHRnl8zENOD5SbrI4HAazss9xBVpikAgxdXKvlxmhfNoYIK0YYnO84MXKkMUinjPQ7zWHbh5lImp7g9HpIXgtkFFTXVvCaTr8mQXXOl957dxePeUvPv28GUdnzXTzk7thTbsWAtqU7YaK4QC4z9qHpbt5ex9ck8uHz2RoptFw71RIoKGiPsBD9YwXAS19goDM2H0yzVtDNJ6ls6jzXrGlJ6gIRG73Er0tVyourPdM42a5oDihfVP6XxjOjS0cmVIIppDSZIofkRfRhQWAunheFbEEPSHx3eybQ6pSIFd34Natgr2erFjyxFIRr7J535HT9aIReYIlocKK2ZI9sfcwhX0PeDNohY2tvHbsrHE0MlKCyVSTjPxszvFjCPlyqwQy')", "in", withTbName = false)),
    select("tp_text") + where(binaryOpWithName("tp_text", "('一般的文字', 'dQWD3XwSTevpbP5hADFdNO0dQvaueFhnGcJAm045mGv5fXttso')", "in", withTbName = false))
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

    colSet.diff(skipLocalSet).map((str: String) =>
      select(distinct(str)) + orderBy(str)
    ).toList
  }

  /**
    * We create test for each type, each operator
    *
    * @return
    */
  def createSymmetryTypeTestCases: List[String] = {
    compareOpList.flatMap((op: String) => {
      colSet.map((tp: String) =>
        select(tp, tp) + where(binaryOpWithName(tp, tp, op, withTbName = false)) + limit())
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

    compareOpList.flatMap((op: String) =>
      colSet.diff(skipLocalSet).flatMap((lCol: String) =>
        colSet.diff(skipLocalSet).map((rCol: String) =>
          buildBinarySelfJoinQuery(lCol, rCol, op)
        )
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

    compareOpList.flatMap((op: String) =>
      colSet.flatMap((lCol: String) =>
        colSet.filter((rCol: String) =>
          lCol.eq(rCol) || (!skipLocalSet.contains(lCol) && !skipLocalSet.contains(rCol)))
          .map((rCol: String) =>
            select(lCol, rCol) + where(binaryOpWithName(lCol, rCol, op, withTbName = false)) + orderBy(ID_COL) + limit()
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
              res += select(countId()) + where(binaryOpWithName(
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