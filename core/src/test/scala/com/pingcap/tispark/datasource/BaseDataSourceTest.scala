package com.pingcap.tispark.datasource

import java.sql.Statement

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{QueryTest, Row}

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

  protected def batchWrite(rows: List[Row],
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

  protected def testSelect(expectedAnswer: Seq[Row], sortCol: String = "i"): Unit = {
    val df = sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .load()
      .sort(sortCol)

    checkAnswer(df, expectedAnswer)
  }

  protected def testFilter(filter: String, expectedAnswer: Seq[Row]): Unit = {
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
