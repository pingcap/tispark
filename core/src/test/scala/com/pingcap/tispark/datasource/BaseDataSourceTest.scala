package com.pingcap.tispark.datasource

import java.util.Objects

import com.pingcap.tikv.TiSession
import com.pingcap.tispark.TiConfigConst
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{BaseTiSparkTest, DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

// Two modes:
// 1. without TiExtensions:
// set isTidbConfigPropertiesInjectedToSparkEnabled = false
// will not load tidb_config.properties to SparkConf
// 2. with TiExtensions
// set isTidbConfigPropertiesInjectedToSparkEnabled = true
// will load tidb_config.properties to SparkConf
class BaseDataSourceTest(val table: String,
                         val database: String = "tispark_test",
                         val _enableTidbConfigPropertiesInjectedToSpark: Boolean = true)
    extends BaseTiSparkTest {
  protected def dbtable = s"`$database`.`$table`"

  override def beforeAll(): Unit = {
    enableTidbConfigPropertiesInjectedToSpark = _enableTidbConfigPropertiesInjectedToSpark
    super.beforeAllWithoutLoadData()
    initializeTimeZone()
  }

  protected def jdbcUpdate(query: String): Unit =
    tidbStmt.execute(query)

  protected def dropTable(): Unit = {
    jdbcUpdate(s"drop table if exists $dbtable")
    // If we reuse tiSession, cache in catalog will be outdated after dropping and creating table.
    if (enableTidbConfigPropertiesInjectedToSpark) {
      ti.tiSession.getCatalog.reloadCache(true)
    }
  }

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

  protected def testTiDBSelect(expectedAnswer: Seq[Row],
                               sortCol: String = "i",
                               selectCol: String = null): Unit = {
    // check data source result & expected answer
    var df = queryDatasourceTiDB(sortCol)
    if (selectCol != null) {
      df = df.select(selectCol)
    }
    checkAnswer(df, expectedAnswer)

    // check table scan
    var df2 = queryDatasourceTableScan(sortCol)
    if (selectCol != null) {
      df2 = df2.select(selectCol)
    }
    checkAnswer(df2, expectedAnswer)
  }

  protected def compareTiDBWriteFailureWithJDBC(
    data: List[Row],
    schema: StructType,
    jdbcErrorClass: Class[_],
    jdbcErrorMsg: String,
    tidbErrorClass: Class[_],
    tidbErrorMsg: String,
    msgStartWith: Boolean = false
  ): Unit = {
    val caughtJDBC = intercept[SparkException] {
      this.jdbcWrite(data, schema)
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
      this.tidbWrite(data, schema)
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
    testCode: ((List[Row], StructType, Option[Map[String, String]]) => Unit, String) => Unit
  ): Unit = {
    testCode(tidbWrite, "tidbWrite")
    testCode(jdbcWrite, "jdbcWrite")
  }

  protected def compareTiDBSelectWithJDBC(expectedAnswer: Seq[Row],
                                          schema: StructType,
                                          sortCol: String = "i",
                                          skipTiDBAndExpectedAnswerCheck: Boolean = false,
                                          skipJDBCReadCheck: Boolean = false): Unit = {
    val sql = s"select * from $dbtable order by $sortCol"
    val answer = seqRowToList(expectedAnswer, schema)

    val jdbcResult = queryTiDBViaJDBC(sql)
    val df = queryDatasourceTiDB(sortCol)
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

  protected def compareTiDBSelectWithJDBC_V2(sortCol: String = "i"): Unit = {
    val sql = s"select * from $dbtable order by $sortCol"

    // check jdbc result & data source result
    val jdbcResult = queryTiDBViaJDBC(sql)
    val df = queryDatasourceTiDB(sortCol)
    val tidbResult = seqRowToList(df.collect(), df.schema)

    assert(
      compSqlResult(sql, jdbcResult, tidbResult, checkLimit = false)
    )
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

  protected def queryDatasourceTableScan(sortCol: String): DataFrame = {
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .option(TiConfigConst.ALLOW_INDEX_READ, "false")
      .load()
      .sort(sortCol)
  }

  protected def queryDatasourceTiDB(sortCol: String): DataFrame =
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", table)
      .load()
      .sort(sortCol)

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
