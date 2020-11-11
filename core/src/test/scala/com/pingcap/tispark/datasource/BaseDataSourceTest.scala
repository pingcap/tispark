/*
 * Copyright 2020 PingCAP, Inc.
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
 */

package com.pingcap.tispark.datasource

import java.util.Objects

import com.pingcap.tikv.allocator.RowIDAllocator
import com.pingcap.tikv.meta.TiColumnInfo
import com.pingcap.tispark.{TiConfigConst, TiDBUtils}
import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{BaseTiSparkTest, DataFrame, Row}

import scala.collection.mutable.ArrayBuffer

class BaseDataSourceTest(val table: String, val database: String = "tispark_test")
    extends BaseTiSparkTest {

  protected def databaseWithPrefix = s"$dbPrefix$database"

  protected def dbtableWithPrefix = s"`$dbPrefix$database`.`$table`"

  protected def dropTable(): Unit = {
    jdbcUpdate(s"drop table if exists $dbtable")
  }

  protected def dropTable(tblName: String): Unit = {
    jdbcUpdate(s"drop table if exists `$database`.`$tblName`")
  }

  protected def jdbcUpdate(query: String): Unit =
    tidbStmt.execute(query)

  protected def testTiDBSelect(
      expectedAnswer: Seq[Row],
      sortCol: String = "i",
      selectCol: String = null): Unit = {
    testTiDBSelectWithTable(expectedAnswer, sortCol, selectCol, table)
  }

  protected def testTiDBSelectWithTable(
      expectedAnswer: Seq[Row],
      sortCol: String = "i",
      selectCol: String = null,
      tableName: String): Unit = {
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

  protected def compareTiDBWriteFailureWithJDBC(
      data: List[Row],
      schema: StructType,
      jdbcErrorClass: Class[_],
      tidbErrorClass: Class[_],
      tidbErrorMsg: String,
      msgStartWith: Boolean = false): Unit = {
    val caughtJDBC = intercept[SparkException] {
      this.jdbcWrite(data, schema)
    }
    assert(
      caughtJDBC.getCause.getClass.equals(jdbcErrorClass),
      s"${caughtJDBC.getCause.getClass.getName} not equals to ${jdbcErrorClass.getName}")

    val caughtTiDB = intercept[SparkException] {
      this.tidbWrite(data, schema)
    }
    assert(
      caughtTiDB.getCause.getClass.equals(tidbErrorClass),
      s"${caughtTiDB.getCause.getClass.getName} not equals to ${tidbErrorClass.getName}")

    if (tidbErrorMsg != null) {
      if (!msgStartWith) {
        assert(
          Objects.equals(caughtTiDB.getCause.getMessage, tidbErrorMsg),
          s"${caughtTiDB.getCause.getMessage} not equals to $tidbErrorMsg")
      } else {
        assert(
          startWith(caughtTiDB.getCause.getMessage, tidbErrorMsg),
          s"${caughtTiDB.getCause.getMessage} not start with $tidbErrorMsg")
      }
    }
  }

  protected def tidbWrite(
      rows: List[Row],
      schema: StructType,
      param: Option[Map[String, String]] = None): Unit = {
    tidbWriteWithTable(rows, schema, table, param)
  }

  protected def tidbWriteWithTable(
      rows: List[Row],
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

  protected def jdbcWrite(
      rows: List[Row],
      schema: StructType,
      param: Option[Map[String, String]] = None): Unit = {
    val data: RDD[Row] = sc.makeRDD(rows)
    val df = sqlContext.createDataFrame(data, schema)
    df.write
      .format("jdbc")
      .option(JDBCOptions.JDBC_URL, jdbcUrl)
      .option(JDBCOptions.JDBC_DRIVER_CLASS, TiDBUtils.TIDB_DRIVER_CLASS)
      .option(JDBCOptions.JDBC_TABLE_NAME, dbtable)
      .option(JDBCOptions.JDBC_TXN_ISOLATION_LEVEL, "REPEATABLE_READ")
      .mode("append")
      .save()
  }

  protected def dbtable = s"`$database`.`$table`"

  private def startWith(a: String, b: String): Boolean = {
    (a == null && b == null) || a.startsWith(b)
  }

  protected def compareTiDBSelectWithJDBC(
      expectedAnswer: List[Row],
      schema: StructType,
      sortCol: String = "i",
      skipTiDBAndExpectedAnswerCheck: Boolean = false,
      skipJDBCReadCheck: Boolean = false): Unit = {
    val sql = s"select * from $dbtable order by $sortCol"
    val answer = seqRowToList(expectedAnswer, schema)

    val jdbcResult = queryTiDBViaJDBC(sql)
    val df =
      try {
        queryDatasourceTiDB(sortCol)
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
      assert(compSqlResult(sql, jdbcResult, tidbResult, checkLimit = false))
    }

  }

  protected def queryDatasourceTiDB(sortCol: String): DataFrame =
    queryDatasourceTiDBWithTable(sortCol, table)

  protected def seqRowToList(rows: Seq[Row], schema: StructType): List[List[Any]] =
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

  protected def queryDatasourceTiDBWithTable(sortCol: String, tableName: String): DataFrame =
    sqlContext.read
      .format("tidb")
      .options(tidbOptions)
      .option("database", database)
      .option("table", tableName)
      .load()
      .sort(sortCol)

  protected def queryDatasourceTableScan(sortCol: String): DataFrame = {
    queryDatasourceTableScanWithTable(sortCol, table)
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

  protected def tiRowToSparkRow(row: TiRow, tiColsInfos: java.util.List[TiColumnInfo]): Row = {
    val sparkRow = new Array[Any](row.fieldCount())
    for (i <- 0 until row.fieldCount()) {
      val colTp = tiColsInfos.get(i).getType
      val colVal = row.get(i, colTp)
      sparkRow(i) = colVal
    }
    Row.fromSeq(sparkRow)
  }

  protected def getTestDatabaseNameInSpark(database: String): String = s"$dbPrefix$database"

  protected def getLongBinaryString(v: Long): String = {
    var str = java.lang.Long.toBinaryString(v)
    while (str.length < 64) {
      str = "0" + str
    }
    "0x" + str
  }

  protected def allocateID(size: Long): RowIDAllocator = {
    val tiDBInfo = ti.tiSession.getCatalog.getDatabase(databaseWithPrefix)
    val tiTableInfo = ti.tiSession.getCatalog.getTable(databaseWithPrefix, table)
    RowIDAllocator.create(
      tiDBInfo.getId,
      tiTableInfo,
      ti.tiSession.getConf,
      tiTableInfo.isAutoIncColUnsigned,
      size)
  }
}
