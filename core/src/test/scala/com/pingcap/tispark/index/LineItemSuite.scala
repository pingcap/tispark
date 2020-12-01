/*
 * Copyright 2019 PingCAP, Inc.
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

package com.pingcap.tispark.index

import com.pingcap.tispark.write.{TiBatchWrite, TiDBOptions}
import org.apache.spark.sql.BaseTiSparkEnableBatchWriteTest
import org.apache.spark.sql.functions._

class LineItemSuite extends BaseTiSparkEnableBatchWriteTest {

  private val table = "LINEITEM"
  private val where = "where L_PARTKEY < 10"
  private val batchWriteTablePrefix = "BATCH.WRITE"
  private val isPkHandlePrefix = "isPkHandle"
  private val replacePKHandlePrefix = "replacePKHandle"
  private val replaceUniquePrefix = "replaceUnique"
  private var database: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    database = tpchDBName
    setCurrentDatabase(database)
    val tableToWrite = s"${batchWriteTablePrefix}_$table"
    tidbStmt.execute(s"drop table if exists `$tableToWrite`")
    tidbStmt.execute(s"create table if not exists `$tableToWrite` like $table ")
  }

  ignore("ti batch write: lineitem") {
    val tableToWrite = s"${batchWriteTablePrefix}_$table"

    // select
    refreshConnections(TestTables(database, tableToWrite))
    val df = sql(s"select * from $table $where")

    // batch write
    TiBatchWrite.write(
      df,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true", "regionSplitMethod" -> "v2")))

    // refresh
    refreshConnections(TestTables(database, tableToWrite))
    setCurrentDatabase(database)

    // select
    queryTiDBViaJDBC(s"select * from `$tableToWrite`")

    // assert
    val originCount =
      queryTiDBViaJDBC(s"select count(*) from $table $where").head.head.asInstanceOf[Long]
    val count =
      queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
    assert(count == originCount)
  }

  ignore("ti batch write: isPkHandle: lineitem") {
    val tableToWrite = s"${batchWriteTablePrefix}_${isPkHandlePrefix}_$table"
    tidbStmt.execute(s"drop table if exists `$tableToWrite`")
    val createTableSQL =
      s"""
         |CREATE TABLE `$tableToWrite` (
         |  `FAKEKEY` bigint(20) not NULL,
         |  `L_ORDERKEY` int(11) NOT NULL,
         |  `L_PARTKEY` int(11) NOT NULL,
         |  `L_SUPPKEY` int(11) NOT NULL,
         |  `L_LINENUMBER` int(11) NOT NULL,
         |  `L_QUANTITY` decimal(15,2) NOT NULL,
         |  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
         |  `L_DISCOUNT` decimal(15,2) NOT NULL,
         |  `L_TAX` decimal(15,2) NOT NULL,
         |  `L_RETURNFLAG` char(1) NOT NULL,
         |  `L_LINESTATUS` char(1) NOT NULL,
         |  `L_SHIPDATE` date NOT NULL,
         |  `L_COMMITDATE` date NOT NULL,
         |  `L_RECEIPTDATE` date NOT NULL,
         |  `L_SHIPINSTRUCT` char(25) NOT NULL,
         |  `L_SHIPMODE` char(10) NOT NULL,
         |  `L_COMMENT` varchar(44) NOT NULL,
         |  PRIMARY KEY `idx_fake_key` (`FAKEKEY`)
         |)
       """.stripMargin

    tidbStmt.execute(createTableSQL)
    val df = sql(s"select * from $table $where")
      .withColumn("FAKEKEY", expr("monotonically_increasing_id()"))

    TiBatchWrite.write(
      df,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true")))

    // select
    queryTiDBViaJDBC(s"select * from `$tableToWrite`")

    // assert
    val originCount =
      queryTiDBViaJDBC(s"select count(*) from $table $where").head.head.asInstanceOf[Long]
    val count =
      queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
    assert(count == originCount)

    // assert
    val selectColumns = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_SHIPDATE, L_COMMENT"
    val diff = queryTiDBViaJDBC(s"""
                                   |SELECT $selectColumns
                                   |FROM (
                                   |    SELECT $selectColumns FROM `$table` $where
                                   |    UNION ALL
                                   |    SELECT $selectColumns FROM `$tableToWrite`
                                   |) tbl
                                   |GROUP BY $selectColumns
                                   |HAVING count(*) != 2""".stripMargin)
    assert(diff.isEmpty)
  }

  ignore("ti batch write: replace + isPkHandle: lineitem") {
    val tableToWrite = s"${batchWriteTablePrefix}_${replacePKHandlePrefix}_$table"
    tidbStmt.execute(s"drop table if exists `$tableToWrite`")
    val createTableSQL =
      s"""
         |CREATE TABLE `$tableToWrite` (
         |  `FAKEKEY` bigint(20) not NULL,
         |  `L_ORDERKEY` int(11) NOT NULL,
         |  `L_PARTKEY` int(11) NOT NULL,
         |  `L_SUPPKEY` int(11) NOT NULL,
         |  `L_LINENUMBER` int(11) NOT NULL,
         |  `L_QUANTITY` decimal(15,2) NOT NULL,
         |  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
         |  `L_DISCOUNT` decimal(15,2) NOT NULL,
         |  `L_TAX` decimal(15,2) NOT NULL,
         |  `L_RETURNFLAG` char(1) NOT NULL,
         |  `L_LINESTATUS` char(1) NOT NULL,
         |  `L_SHIPDATE` date NOT NULL,
         |  `L_COMMITDATE` date NOT NULL,
         |  `L_RECEIPTDATE` date NOT NULL,
         |  `L_SHIPINSTRUCT` char(25) NOT NULL,
         |  `L_SHIPMODE` char(10) NOT NULL,
         |  `L_COMMENT` varchar(50) NOT NULL,
         |  PRIMARY KEY (`FAKEKEY`)
         |)
       """.stripMargin

    tidbStmt.execute(createTableSQL)
    val df1 = sql(s"select * from $table $where")
      .withColumn("FAKEKEY", expr("monotonically_increasing_id()"))
    val dfCount1 = df1.count().toInt

    TiBatchWrite.write(
      df1,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true")))

    val tmpView = "lineitem_tempview"
    df1.createOrReplaceGlobalTempView(tmpView)
    val dfCount2 = dfCount1 / 2
    val df2 = sql(
      s"select FAKEKEY, L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, CONCAT(L_COMMENT, '_t') AS L_COMMENT from global_temp.`$tmpView` limit $dfCount2")

    TiBatchWrite.write(
      df2,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "replace" -> "true", "isTest" -> "true")))

    // select
    queryTiDBViaJDBC(s"select * from `$tableToWrite`")

    // assert
    val originCount =
      queryTiDBViaJDBC(s"select count(*) from $table $where").head.head.asInstanceOf[Long]
    val count =
      queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
    assert(count == originCount)

    // assert
    val selectColumns = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_SHIPDATE, L_COMMENT"
    val diff = queryTiDBViaJDBC(s"""
                                   |SELECT $selectColumns
                                   |FROM (
                                   |    SELECT $selectColumns FROM `$table` $where
                                   |    UNION ALL
                                   |    SELECT $selectColumns FROM `$tableToWrite`
                                   |) tbl
                                   |GROUP BY $selectColumns
                                   |HAVING count(*) != 2""".stripMargin)
    assert(diff.lengthCompare(dfCount2 * 2) == 0)
  }

  ignore("ti batch write: replace + uniqueKey: lineitem") {
    val tableToWrite = s"${batchWriteTablePrefix}_${replaceUniquePrefix}_$table"
    tidbStmt.execute(s"drop table if exists `$tableToWrite`")
    val createTableSQL =
      s"""
         |CREATE TABLE `$tableToWrite` (
         |  `FAKEKEY` bigint(20) not NULL,
         |  `L_ORDERKEY` int(11) NOT NULL,
         |  `L_PARTKEY` int(11) NOT NULL,
         |  `L_SUPPKEY` int(11) NOT NULL,
         |  `L_LINENUMBER` int(11) NOT NULL,
         |  `L_QUANTITY` decimal(15,2) NOT NULL,
         |  `L_EXTENDEDPRICE` decimal(15,2) NOT NULL,
         |  `L_DISCOUNT` decimal(15,2) NOT NULL,
         |  `L_TAX` decimal(15,2) NOT NULL,
         |  `L_RETURNFLAG` char(1) NOT NULL,
         |  `L_LINESTATUS` char(1) NOT NULL,
         |  `L_SHIPDATE` date NOT NULL,
         |  `L_COMMITDATE` date NOT NULL,
         |  `L_RECEIPTDATE` date NOT NULL,
         |  `L_SHIPINSTRUCT` char(25) NOT NULL,
         |  `L_SHIPMODE` char(10) NOT NULL,
         |  `L_COMMENT` varchar(50) NOT NULL,
         |  UNIQUE KEY `idx_fake_key` (`FAKEKEY`)
         |)
       """.stripMargin

    tidbStmt.execute(createTableSQL)
    val df1 = sql(s"select * from $table $where")
      .withColumn("FAKEKEY", expr("monotonically_increasing_id()"))
    val dfCount1 = df1.count().toInt

    TiBatchWrite.write(
      df1,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true")))

    val tmpView = "lineitem_tempview"
    df1.createOrReplaceGlobalTempView(tmpView)
    val dfCount2 = dfCount1 / 2
    val df2 = sql(
      s"select FAKEKEY, L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, CONCAT(L_COMMENT, '_t') AS L_COMMENT from global_temp.`$tmpView` limit $dfCount2")

    TiBatchWrite.write(
      df2,
      ti,
      new TiDBOptions(
        tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "replace" -> "true", "isTest" -> "true")))

    // select
    queryTiDBViaJDBC(s"select * from `$tableToWrite`")

    // assert
    val originCount =
      queryTiDBViaJDBC(s"select count(*) from $table $where").head.head.asInstanceOf[Long]
    val count =
      queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
    assert(count == originCount)

    // assert
    val selectColumns = "L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_QUANTITY, L_SHIPDATE, L_COMMENT"
    val diff = queryTiDBViaJDBC(s"""
                                   |SELECT $selectColumns
                                   |FROM (
                                   |    SELECT $selectColumns FROM `$table` $where
                                   |    UNION ALL
                                   |    SELECT $selectColumns FROM `$tableToWrite`
                                   |) tbl
                                   |GROUP BY $selectColumns
                                   |HAVING count(*) != 2""".stripMargin)
    assert(diff.lengthCompare(dfCount2 * 2) == 0)
  }

  override def afterAll(): Unit = {
    try {
      setCurrentDatabase(database)

      {
        val tableToWrite = s"${batchWriteTablePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

      {
        val tableToWrite = s"${batchWriteTablePrefix}_${isPkHandlePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

      {
        val tableToWrite = s"${batchWriteTablePrefix}_${replacePKHandlePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

      {
        val tableToWrite = s"${batchWriteTablePrefix}_${replaceUniquePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

    } finally {
      super.afterAll()
    }
  }

}
