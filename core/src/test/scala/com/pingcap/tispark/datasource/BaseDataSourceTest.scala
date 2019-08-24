package com.pingcap.tispark.datasource

import java.util.Objects

import com.pingcap.tispark.TiConfigConst
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{BaseTiSparkTest, DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

// Two modes:
// 1. without TiExtensions:
// set isTidbConfigPropertiesInjectedToSparkEnabled = false
// will not load tidb_config.properties to SparkConf
// 2. with TiExtensions
// set isTidbConfigPropertiesInjectedToSparkEnabled = true
// will load tidb_config.properties to SparkConf
class BaseDataSourceTest(val database: String = "tispark_test",
                         val _enableTidbConfigPropertiesInjectedToSpark: Boolean = true)
    extends BaseTiSparkTest {

  override def beforeAll(): Unit = {
    enableTidbConfigPropertiesInjectedToSpark = _enableTidbConfigPropertiesInjectedToSpark
    super.beforeAllWithoutLoadData()
    initializeTimeZone()
  }

  def createTable(queryTemplate: String, table: String): Unit = {
    jdbcUpdate(queryTemplate.format(database, table))
    tables += table
  }
  protected def jdbcUpdate(queryTemplate: String, table: String): Unit =
    tidbStmt.execute(queryTemplate.format(database, table))

  protected def jdbcUpdate(query: String): Unit =
    tidbStmt.execute(query)

  protected def dropTable(tblName: String): Unit = {
    jdbcUpdate(s"drop table if exists `$database`.`$tblName`")
  }

  protected def tidbWriteWithTable(rows: List[Row],
                                   schema: StructType,
                                   tblName: String,
                                   param: Option[Map[String, String]] = None): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions ++ param.getOrElse(Map.empty))
      .option("database", database)
      .option("table", tblName)
      .mode("append")
      .save()
  }

  protected def jdbcWrite(rows: List[Row],
                          schema: StructType,
                          table: String,
                          param: Option[Map[String, String]] = None): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", s"`$database`.`$table`")
      .option("isolationLevel", "REPEATABLE_READ")
      .mode("append")
      .save()
  }

  protected def testTiDBSelectWithTable(
    expectedAnswer: Seq[Row],
    sortCol: String = "i",
    selectCol: String = null,
    tableName: String
  ): Unit = {
    // check data source result & expected answer
    var df = queryDatasourceTiDBWithTable(sortCol, tableName)
    if (selectCol != null) {
      df = df.select(selectCol)
    }
    checkAnswer(df, expectedAnswer)

    // check table scan
    var df2 = queryDatasourceTableScanWithTable(sortCol, tableName)
    if (selectCol != null) {
      df2 = df2.select(selectCol)
    }
    checkAnswer(df2, expectedAnswer)
  }

  protected def compareTiDBWriteFailureWithJDBC(
    data: List[Row],
    schema: StructType,
    table: String,
    jdbcErrorClass: Class[_],
    jdbcErrorMsg: String,
    tidbErrorClass: Class[_],
    tidbErrorMsg: String,
    msgStartWith: Boolean = false
  ): Unit = {
    val caughtJDBC = intercept[SparkException] {
      this.jdbcWrite(data, schema, table)
    }
    assert(
      caughtJDBC.getCause.getClass.equals(jdbcErrorClass),
      s"${caughtJDBC.getCause.getClass.getName} not equals to ${jdbcErrorClass.getName}"
    )

    if (!msgStartWith) {
      assert(
        Objects.equals(caughtJDBC.getCause.getMessage, jdbcErrorMsg),
        s"${caughtJDBC.getCause.getMessage} not equals to $jdbcErrorMsg"
      )
    } else {
      assert(
        startWith(caughtJDBC.getCause.getMessage, jdbcErrorMsg),
        s"${caughtJDBC.getCause.getMessage} not start with $jdbcErrorMsg"
      )
    }

    val caughtTiDB = intercept[SparkException] {
      this.tidbWriteWithTable(data, schema, table)
    }
    assert(
      caughtTiDB.getCause.getClass.equals(tidbErrorClass),
      s"${caughtTiDB.getCause.getClass.getName} not equals to ${tidbErrorClass.getName}"
    )

    if (!msgStartWith) {
      assert(
        Objects.equals(caughtTiDB.getCause.getMessage, tidbErrorMsg),
        s"${caughtTiDB.getCause.getMessage} not equals to $tidbErrorMsg"
      )
    } else {
      assert(
        startWith(caughtTiDB.getCause.getMessage, tidbErrorMsg),
        s"${caughtTiDB.getCause.getMessage} not start with $tidbErrorMsg"
      )
    }
  }

  private def startWith(a: String, b: String): Boolean = {
    (a == null && b == null) || a.startsWith(b)
  }

  protected def compareTiDBWriteWithJDBC(
    testCode: ((List[Row], StructType, String, Option[Map[String, String]]) => Unit, String) => Unit
  ): Unit = {
    testCode(tidbWriteWithTable, "tidbWrite")
    testCode(jdbcWrite, "jdbcWrite")
  }

  protected def compareTiDBSelectWithJDBC(expectedAnswer: Seq[Row],
                                          schema: StructType,
                                          table: String,
                                          sortCol: String = "i",
                                          skipTiDBAndExpectedAnswerCheck: Boolean = false,
                                          skipJDBCReadCheck: Boolean = false): Unit = {
    val sql = s"select * from `$database`.`$table` order by $sortCol"
    val answer = seqRowToList(expectedAnswer, schema)

    val jdbcResult = queryTiDBViaJDBC(sql)
    val df = try {
      queryDatasourceTiDBWithTable(sortCol, table)
    } catch {
      case e: NoSuchTableException =>
        logger.warn("query via datasource api fails", e)
        spark.sql("show tables").show
        throw e
    }
    val tidbResult = seqRowToList(df.collect(), df.schema)

    // check tidb result & expected answer
    if (!skipTiDBAndExpectedAnswerCheck) {
      checkAnswer(df, expectedAnswer)
    }

    if (!skipJDBCReadCheck) {
      // check jdbc result & expected answer
      assert(compSqlResult(sql, jdbcResult, answer, checkLimit = false))

      // check jdbc result & tidb result
      assert(
        compSqlResult(sql, jdbcResult, tidbResult, checkLimit = false)
      )
    }

  }

  protected def compareTiDBSelectWithJDBCWithTable_V2(tblName: String,
                                                      sortCol: String = "i"): Unit = {
    val sql = s"select * from `$database`.`$tblName` order by $sortCol"

    // check jdbc result & data source result
    val jdbcResult = queryTiDBViaJDBC(sql)
    val df = queryDatasourceTiDBWithTable(sortCol, tableName = tblName)
    val tidbResult = seqRowToList(df.collect(), df.schema)

    if (!compResult(jdbcResult, tidbResult)) {
      logger.error(s"""Failed on $tblName\n
                      |DataSourceAPI result: ${listToString(jdbcResult)}\n
                      |TiDB via JDBC result: ${listToString(tidbResult)}""".stripMargin)
      fail()
    }
  }

  private def seqRowToList(rows: Seq[Row], schema: StructType): List[List[Any]] =
    rows
      .map(row => {
        val rowRes = ArrayBuffer.empty[Any]
        for (i <- 0 until row.length) {
          if (row.get(i) == null) {
            rowRes += null
          } else {
            rowRes += toOutput(row.get(i), schema(i).dataType.typeName)
          }
        }
        rowRes.toList
      })
      .toList

  protected def queryDatasourceTableScanWithTable(sortCol: String, tblName: String): DataFrame = {
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", tblName)
      .option(TiConfigConst.ALLOW_INDEX_READ, "false")
      .load()
      .sort(sortCol)
  }

  protected def queryDatasourceTiDBWithTable(sortCol: String, tableName: String): DataFrame =
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", tableName)
      .load()
      .sort(sortCol)

  protected def testTiDBSelectFilter(table: String,
                                     filter: String,
                                     expectedAnswer: Seq[Row]): Unit = {
    val loadedDf = sqlContext.read
      .format("tidb")
      .option("database", database)
      .option("table", table)
      .options(tidbOptions)
      .load()
      .filter(filter)
      .sort("i")
    checkAnswer(loadedDf, expectedAnswer)
  }

  protected def getTestDatabaseNameInSpark(database: String): String =
    if (_enableTidbConfigPropertiesInjectedToSpark) {
      s"$dbPrefix$database"
    } else {
      database
    }

  val tables: ListBuffer[String] = mutable.ListBuffer.empty[String]

  override def afterAll(): Unit =
    try {
      tables.foreach(tbl => dropTable(tbl))
    } finally {
      super.afterAll()
    }
}
