package com.pingcap.tispark.datasource

import java.sql.Statement

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, QueryTest, Row}

import scala.collection.mutable.ArrayBuffer

// Tow modes:
// 1. without TiExtensions:
// set isTidbConfigPropertiesInjectedToSparkEnabled = false
// will not load tidb_config.properties to SparkConf
// 2. with TiExtensions
// set isTidbConfigPropertiesInjectedToSparkEnabled = true
// will load tidb_config.properties to SparkConf
class BaseDataSourceTest(val table: String,
                         val _enableTidbConfigPropertiesInjectedToSpark: Boolean = true)
    extends QueryTest
    with SharedSQLContext {
  protected val database: String = "tispark_test"
  protected val dbtable = s"$database.$table"

  protected var tidbStmt: Statement = _

  override def beforeAll(): Unit = {
    enableTidbConfigPropertiesInjectedToSpark = _enableTidbConfigPropertiesInjectedToSpark

    super.beforeAll()

    tidbStmt = tidbConn.createStatement()
  }

  protected def jdbcUpdate(query: String): Unit =
    tidbStmt.execute(query)

  protected def dropTable(): Unit = jdbcUpdate(s"drop table if exists $dbtable")

  protected def tidbWrite(rows: List[Row],
                          schema: StructType,
                          param: Option[Map[String, String]] = None): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("tidb")
      .options(tidbOptions ++ param.getOrElse(Map.empty))
      .option("database", database)
      .option("table", table)
      .mode("append")
      .save()
  }

  protected def jdbcWrite(rows: List[Row],
                          schema: StructType,
                          param: Option[Map[String, String]] = None): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", dbtable)
      .option("isolationLevel", "REPEATABLE_READ")
      .mode("append")
      .save()
  }

  protected def testTiDBSelect(expectedAnswer: Seq[Row], sortCol: String = "i"): Unit = {
    // check data source result & expected answer
    val df = queryTiDB(sortCol)
    checkAnswer(df, expectedAnswer)
  }

  protected def compareTiDBWriteWithJDBC(
    testCode: ((List[Row], StructType, Option[Map[String, String]]) => Unit, String) => Unit
  ): Unit = {
    testCode(jdbcWrite, "jdbcWrite")
    testCode(tidbWrite, "tidbWrite")
  }

  protected def compareTiDBSelectWithJDBC(expectedAnswer: Seq[Row],
                                          schema: StructType,
                                          sortCol: String = "i"): Unit = {
    val sql = s"select * from $dbtable order by $sortCol"
    val answer = seqRowToList(expectedAnswer, schema)

    // check jdbc result & expected answer
    val jdbcResult = queryJDBC(sql)
    compSqlResult(sql, jdbcResult, answer, checkLimit = false)

    // check data source result & expected answer
    val df = queryTiDB(sortCol)
    compSqlResult(sql, seqRowToList(df.collect(), df.schema), answer, checkLimit = false)
  }

  protected def compareTiDBSelectWithJDBC_V2(sortCol: String = "i"): Unit = {
    val sql = s"select * from $dbtable order by $sortCol"

    // check jdbc result & data source result
    val jdbcResult = queryJDBC(sql)
    val df = queryTiDB(sortCol)

    compSqlResult(sql, jdbcResult, seqRowToList(df.collect(), df.schema), checkLimit = false)
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

  private def queryTiDB(sortCol: String): DataFrame =
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .load()
      .sort(sortCol)

  private def queryJDBC(query: String): List[List[Any]] = {
    val resultSet = tidbStmt.executeQuery(query)
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

  protected def testTiDBSelectFilter(filter: String, expectedAnswer: Seq[Row]): Unit = {
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
}
