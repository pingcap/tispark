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

package com.pingcap.tispark

import com.pingcap.tispark.write.{TiBatchWrite, TiDBOptions}
import org.apache.spark.sql.BaseTiSparkEnableBatchWriteTest
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException

class TiBatchWriteSuite extends BaseTiSparkEnableBatchWriteTest {

  private val tables =
    "CUSTOMER" ::
      //"LINEITEM" :: to large for test
      "NATION" ::
      "ORDERS" ::
      "PART" ::
      "PARTSUPP" ::
      "REGION" ::
      "SUPPLIER" ::
      Nil
  private val batchWriteTablePrefix = "BATCH.WRITE"
  private val isPkHandlePrefix = "isPkHandle"
  private val replacePKHandlePrefix = "replacePKHandle"
  private val replaceUniquePrefix = "replaceUnique"
  private var database: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    database = tpchDBName
    setCurrentDatabase(database)
    for (table <- tables) {
      val tableToWrite = s"${batchWriteTablePrefix}_$table"
      tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      tidbStmt.execute(s"create table if not exists `$tableToWrite` like $table ")
    }
  }

  test("ti batch write") {
    for (table <- tables) {
      logDebug(s"start test table [$table]")

      val tableToWrite = s"${batchWriteTablePrefix}_$table"

      // select
      refreshConnections(TestTables(database, tableToWrite))
      val df = sql(s"select * from $table")

      // batch write
      TiBatchWrite.write(
        df,
        ti,
        new TiDBOptions(
          tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true")))

      // select
      queryTiDBViaJDBC(s"select * from `$tableToWrite`")

      // assert
      val originCount =
        queryTiDBViaJDBC(s"select count(*) from $table").head.head.asInstanceOf[Long]
      val count =
        queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
      assert(count == originCount)
    }
  }

  test("ti batch write: isPkHandle") {
    for (table <- tables) {
      logDebug(s"start test table [$table]")
      val tableToWrite = s"${batchWriteTablePrefix}_${isPkHandlePrefix}_$table"
      tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      val (createTableSQL, selectColumns) = table match {
        case "CUSTOMER" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `C_CUSTKEY` int(11) NOT NULL,
               |  `C_NAME` varchar(25) NOT NULL,
               |  `C_ADDRESS` varchar(40) NOT NULL,
               |  `C_NATIONKEY` int(11) NOT NULL,
               |  `C_PHONE` char(15) NOT NULL,
               |  `C_ACCTBAL` decimal(15,2) NOT NULL,
               |  `C_MKTSEGMENT` char(10) NOT NULL,
               |  `C_COMMENT` varchar(117) NOT NULL,
               |  PRIMARY KEY(`C_CUSTKEY`)
               |)
        """.stripMargin,
            "C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT")
        case "NATION" =>
          (
            s"""
                             |CREATE TABLE `$tableToWrite` (
                             |  `N_NATIONKEY` int(11) NOT NULL,
                             |  `N_NAME` char(25) NOT NULL,
                             |  `N_REGIONKEY` int(11) NOT NULL,
                             |  `N_COMMENT` varchar(152) DEFAULT NULL,
                             |  PRIMARY KEY (`N_NATIONKEY`)
                             |)
           """.stripMargin,
            "N_NATIONKEY, N_NAME, N_REGIONKEY, N_COMMENT")
        case "ORDERS" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `O_ORDERKEY` int(11) NOT NULL,
               |  `O_CUSTKEY` int(11) NOT NULL,
               |  `O_ORDERSTATUS` char(1) NOT NULL,
               |  `O_TOTALPRICE` decimal(15,2) NOT NULL,
               |  `O_ORDERDATE` date NOT NULL,
               |  `O_ORDERPRIORITY` char(15) NOT NULL,
               |  `O_CLERK` char(15) NOT NULL,
               |  `O_SHIPPRIORITY` int(11) NOT NULL,
               |  `O_COMMENT` varchar(79) NOT NULL,
               |  PRIMARY KEY (`O_ORDERKEY`)
               |)
           """.stripMargin,
            "O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT")
        case "PART" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `P_PARTKEY` int(11) NOT NULL,
               |  `P_NAME` varchar(55) NOT NULL,
               |  `P_MFGR` char(25) NOT NULL,
               |  `P_BRAND` char(10) NOT NULL,
               |  `P_TYPE` varchar(25) NOT NULL,
               |  `P_SIZE` int(11) NOT NULL,
               |  `P_CONTAINER` char(10) NOT NULL,
               |  `P_RETAILPRICE` decimal(15,2) NOT NULL,
               |  `P_COMMENT` varchar(23) NOT NULL,
               |  PRIMARY KEY (`P_PARTKEY`)
               |)
           """.stripMargin,
            "P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT")
        case "REGION" =>
          (
            s"""
                             |CREATE TABLE `$tableToWrite` (
                             |  `R_REGIONKEY` int(11) NOT NULL,
                             |  `R_NAME` char(25) NOT NULL,
                             |  `R_COMMENT` varchar(152) DEFAULT NULL,
                             |  PRIMARY KEY (`R_REGIONKEY`)
                             |)
           """.stripMargin,
            "R_REGIONKEY, R_NAME, R_COMMENT")
        case "SUPPLIER" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `S_SUPPKEY` int(11) NOT NULL,
               |  `S_NAME` char(25) NOT NULL,
               |  `S_ADDRESS` varchar(40) NOT NULL,
               |  `S_NATIONKEY` int(11) NOT NULL,
               |  `S_PHONE` char(15) NOT NULL,
               |  `S_ACCTBAL` decimal(15,2) NOT NULL,
               |  `S_COMMENT` varchar(101) NOT NULL,
               |  PRIMARY KEY (`S_SUPPKEY`)
               |)
           """.stripMargin,
            "S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT")
        case _ => ("", "")
      }

      if (createTableSQL.isEmpty) {
        logDebug(s"skip test table [$table]")
      } else {
        tidbStmt.execute(createTableSQL)
        val df = sql(s"select * from $table")
        TiBatchWrite.write(
          df,
          ti,
          new TiDBOptions(
            tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "isTest" -> "true")))

        queryTiDBViaJDBC(s"select * from `$tableToWrite`")

        val diff = queryTiDBViaJDBC(s"""
                                       |SELECT $selectColumns
                                       |FROM (
                                       |    SELECT $selectColumns FROM `$table`
                                       |    UNION ALL
                                       |    SELECT $selectColumns FROM `$tableToWrite`
                                       |) tbl
                                       |GROUP BY $selectColumns
                                       |HAVING count(*) != 2""".stripMargin)
        assert(diff.isEmpty)
      }
    }
  }

  test("ti batch write: replace + isPkHandle") {
    for (table <- tables) {
      logDebug(s"start test table [$table]")

      val tableToWrite = s"${batchWriteTablePrefix}_${replacePKHandlePrefix}_$table"
      tidbStmt.execute(s"drop table if exists `$tableToWrite`")

      val (createTableSQL, updateColumn, selectColumns, updateSize) = table match {
        case "CUSTOMER" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `C_CUSTKEY` int(11) NOT NULL,
               |  `C_NAME` varchar(25) NOT NULL,
               |  `C_ADDRESS` varchar(40) NOT NULL,
               |  `C_NATIONKEY` int(11) NOT NULL,
               |  `C_PHONE` char(15) NOT NULL,
               |  `C_ACCTBAL` decimal(15,2) NOT NULL,
               |  `C_MKTSEGMENT` char(10) NOT NULL,
               |  `C_COMMENT` varchar(117) NOT NULL,
               |  PRIMARY KEY(`C_CUSTKEY`)
               |)
        """.stripMargin,
            "C_NAME",
            "C_CUSTKEY, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT",
            100)
        case "NATION" =>
          (
            s"""
                             |CREATE TABLE `$tableToWrite` (
                             |  `N_NATIONKEY` int(11) NOT NULL,
                             |  `N_NAME` char(25) NOT NULL,
                             |  `N_REGIONKEY` int(11) NOT NULL,
                             |  `N_COMMENT` varchar(152) DEFAULT NULL,
                             |  PRIMARY KEY (`N_NATIONKEY`)
                             |)
           """.stripMargin,
            "N_NAME",
            "N_NATIONKEY, N_REGIONKEY, N_COMMENT",
            10)
        case "ORDERS" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `O_ORDERKEY` int(11) NOT NULL,
               |  `O_CUSTKEY` int(11) NOT NULL,
               |  `O_ORDERSTATUS` char(1) NOT NULL,
               |  `O_TOTALPRICE` decimal(15,2) NOT NULL,
               |  `O_ORDERDATE` date NOT NULL,
               |  `O_ORDERPRIORITY` char(15) NOT NULL,
               |  `O_CLERK` char(15) NOT NULL,
               |  `O_SHIPPRIORITY` int(11) NOT NULL,
               |  `O_COMMENT` varchar(100) NOT NULL,
               |  PRIMARY KEY (`O_ORDERKEY`)
               |)
           """.stripMargin,
            "O_COMMENT",
            "O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY",
            1000)
        case "PART" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `P_PARTKEY` int(11) NOT NULL,
               |  `P_NAME` varchar(55) NOT NULL,
               |  `P_MFGR` char(25) NOT NULL,
               |  `P_BRAND` char(10) NOT NULL,
               |  `P_TYPE` varchar(25) NOT NULL,
               |  `P_SIZE` int(11) NOT NULL,
               |  `P_CONTAINER` char(10) NOT NULL,
               |  `P_RETAILPRICE` decimal(15,2) NOT NULL,
               |  `P_COMMENT` varchar(23) NOT NULL,
               |  PRIMARY KEY (`P_PARTKEY`)
               |)
           """.stripMargin,
            "P_NAME",
            "P_PARTKEY, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT",
            100)
        case "REGION" =>
          (
            s"""
                             |CREATE TABLE `$tableToWrite` (
                             |  `R_REGIONKEY` int(11) NOT NULL,
                             |  `R_NAME` char(25) NOT NULL,
                             |  `R_COMMENT` varchar(152) DEFAULT NULL,
                             |  PRIMARY KEY (`R_REGIONKEY`)
                             |)
           """.stripMargin,
            "R_NAME",
            "R_REGIONKEY, R_COMMENT",
            3)
        case "SUPPLIER" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `S_SUPPKEY` int(11) NOT NULL,
               |  `S_NAME` char(25) NOT NULL,
               |  `S_ADDRESS` varchar(40) NOT NULL,
               |  `S_NATIONKEY` int(11) NOT NULL,
               |  `S_PHONE` char(15) NOT NULL,
               |  `S_ACCTBAL` decimal(15,2) NOT NULL,
               |  `S_COMMENT` varchar(101) NOT NULL,
               |  PRIMARY KEY (`S_SUPPKEY`)
               |)
           """.stripMargin,
            "S_NAME",
            "S_SUPPKEY, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT",
            5)
        case _ => ("", "", "", 0)
      }

      if (createTableSQL.isEmpty) {
        logDebug(s"skip test table [$table]")
      } else {
        tidbStmt.execute(createTableSQL)
        tidbStmt.execute(s"""insert into `$tableToWrite` select * from `$table`""")

        // select
        val df = sql(
          s"""select $selectColumns, CONCAT($updateColumn, "_t") AS $updateColumn from $table limit $updateSize""")

        // batch write
        TiBatchWrite.write(
          df,
          ti,
          new TiDBOptions(
            tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "replace" -> "true", "isTest" -> "true")))
        // select
        queryTiDBViaJDBC(s"select * from `$tableToWrite`")

        // assert
        val diff1 = queryTiDBViaJDBC(s"""
                                        |SELECT $selectColumns
                                        |FROM (
                                        |    SELECT $selectColumns FROM `$table`
                                        |    UNION ALL
                                        |    SELECT $selectColumns FROM `$tableToWrite`
                                        |) tbl
                                        |GROUP BY $selectColumns
                                        |HAVING count(*) != 2""".stripMargin)
        assert(diff1.isEmpty)

        // assert
        val diff2 = queryTiDBViaJDBC(s"""
                                        |SELECT $selectColumns, $updateColumn
                                        |FROM (
                                        |    SELECT $selectColumns, $updateColumn FROM `$table`
                                        |    UNION ALL
                                        |    SELECT $selectColumns, $updateColumn FROM `$tableToWrite`
                                        |) tbl
                                        |GROUP BY $selectColumns, $updateColumn
                                        |HAVING count(*) != 2""".stripMargin)
        assert(diff2.nonEmpty)
        assert(diff2.lengthCompare(updateSize * 2) == 0)

        val originCount =
          queryTiDBViaJDBC(s"select count(*) from $table").head.head.asInstanceOf[Long]
        val count =
          queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
        assert(count == originCount)
      }
    }
  }

  test("ti batch write: replace + uniqueKey") {
    for (table <- tables) {
      logDebug(s"start test table [$table]")

      val tableToWrite = s"${batchWriteTablePrefix}_${replaceUniquePrefix}_$table"
      tidbStmt.execute(s"drop table if exists `$tableToWrite`")

      val (createTableSQL, updateColumn, selectColumns, updateSize) = table match {
        case "CUSTOMER" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `C_CUSTKEY` int(11) NOT NULL,
               |  `C_NAME` varchar(25) NOT NULL,
               |  `C_ADDRESS` varchar(40) NOT NULL,
               |  `C_NATIONKEY` int(11) NOT NULL,
               |  `C_PHONE` char(15) NOT NULL,
               |  `C_ACCTBAL` decimal(15,2) NOT NULL,
               |  `C_MKTSEGMENT` char(10) NOT NULL,
               |  `C_COMMENT` varchar(117) NOT NULL,
               |  UNIQUE KEY `idx_cust_key` (`C_CUSTKEY`)
               |)
        """.stripMargin,
            "C_NAME",
            "C_CUSTKEY, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT",
            100)
        case "NATION" =>
          (
            s"""
                             |CREATE TABLE `$tableToWrite` (
                             |  `N_NATIONKEY` int(11) NOT NULL,
                             |  `N_NAME` char(25) NOT NULL,
                             |  `N_REGIONKEY` int(11) NOT NULL,
                             |  `N_COMMENT` varchar(152) DEFAULT NULL,
                             |  PRIMARY KEY (`N_NATIONKEY`)
                             |)
           """.stripMargin,
            "N_NAME",
            "N_NATIONKEY, N_REGIONKEY, N_COMMENT",
            10)
        case "ORDERS" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `O_ORDERKEY` int(11) NOT NULL,
               |  `O_CUSTKEY` int(11) NOT NULL,
               |  `O_ORDERSTATUS` char(1) NOT NULL,
               |  `O_TOTALPRICE` decimal(15,2) NOT NULL,
               |  `O_ORDERDATE` date NOT NULL,
               |  `O_ORDERPRIORITY` char(15) NOT NULL,
               |  `O_CLERK` char(15) NOT NULL,
               |  `O_SHIPPRIORITY` int(11) NOT NULL,
               |  `O_COMMENT` varchar(100) NOT NULL,
               |  UNIQUE KEY `idx_order_key` (`O_ORDERKEY`)
               |)
           """.stripMargin,
            "O_COMMENT",
            "O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY",
            1000)
        case "PART" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `P_PARTKEY` int(11) NOT NULL,
               |  `P_NAME` varchar(55) NOT NULL,
               |  `P_MFGR` char(25) NOT NULL,
               |  `P_BRAND` char(10) NOT NULL,
               |  `P_TYPE` varchar(25) NOT NULL,
               |  `P_SIZE` int(11) NOT NULL,
               |  `P_CONTAINER` char(10) NOT NULL,
               |  `P_RETAILPRICE` decimal(15,2) NOT NULL,
               |  `P_COMMENT` varchar(23) NOT NULL,
               |  UNIQUE KEY `idx_part_key` (`P_PARTKEY`)
               |)
           """.stripMargin,
            "P_NAME",
            "P_PARTKEY, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT",
            100)
        case "REGION" =>
          (
            s"""
                             |CREATE TABLE `$tableToWrite` (
                             |  `R_REGIONKEY` int(11) NOT NULL,
                             |  `R_NAME` char(25) NOT NULL,
                             |  `R_COMMENT` varchar(152) DEFAULT NULL,
                             |  UNIQUE KEY `idx_region_key` (`R_REGIONKEY`)
                             |)
           """.stripMargin,
            "R_NAME",
            "R_REGIONKEY, R_COMMENT",
            3)
        case "SUPPLIER" =>
          (
            s"""
               |CREATE TABLE `$tableToWrite` (
               |  `S_SUPPKEY` int(11) NOT NULL,
               |  `S_NAME` char(25) NOT NULL,
               |  `S_ADDRESS` varchar(40) NOT NULL,
               |  `S_NATIONKEY` int(11) NOT NULL,
               |  `S_PHONE` char(15) NOT NULL,
               |  `S_ACCTBAL` decimal(15,2) NOT NULL,
               |  `S_COMMENT` varchar(101) NOT NULL,
               |  UNIQUE KEY `idx_supp_key` (`S_SUPPKEY`)
               |)
           """.stripMargin,
            "S_NAME",
            "S_SUPPKEY, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT",
            5)
        case _ => ("", "", "", 0)
      }

      if (createTableSQL.isEmpty) {
        logDebug(s"skip test table [$table]")
      } else {
        tidbStmt.execute(createTableSQL)

        tidbStmt.execute(s"""insert into `$tableToWrite` select * from `$table`""")

        // select
        val df = sql(
          s"""select $selectColumns, CONCAT($updateColumn, "_t") AS $updateColumn from $table limit $updateSize""")

        // batch write
        TiBatchWrite.write(
          df,
          ti,
          new TiDBOptions(
            tidbOptions + ("database" -> s"$database", "table" -> tableToWrite, "replace" -> "true", "isTest" -> "true")))
        // select
        queryTiDBViaJDBC(s"select * from `$tableToWrite`")

        // assert
        val diff1 = queryTiDBViaJDBC(s"""
                                        |SELECT $selectColumns
                                        |FROM (
                                        |    SELECT $selectColumns FROM `$table`
                                        |    UNION ALL
                                        |    SELECT $selectColumns FROM `$tableToWrite`
                                        |) tbl
                                        |GROUP BY $selectColumns
                                        |HAVING count(*) != 2""".stripMargin)
        assert(diff1.isEmpty)

        // assert
        val diff2 = queryTiDBViaJDBC(s"""
                                        |SELECT $selectColumns, $updateColumn
                                        |FROM (
                                        |    SELECT $selectColumns, $updateColumn FROM `$table`
                                        |    UNION ALL
                                        |    SELECT $selectColumns, $updateColumn FROM `$tableToWrite`
                                        |) tbl
                                        |GROUP BY $selectColumns, $updateColumn
                                        |HAVING count(*) != 2""".stripMargin)
        assert(diff2.nonEmpty)
        assert(diff2.lengthCompare(updateSize * 2) == 0)

        val originCount =
          queryTiDBViaJDBC(s"select count(*) from $table").head.head.asInstanceOf[Long]
        val count =
          queryTiDBViaJDBC(s"select count(*) from `$tableToWrite`").head.head.asInstanceOf[Long]
        assert(count == originCount)
      }
    }
  }

  test("table not exists") {
    // select
    val df = sql(s"select * from CUSTOMER")

    // batch write
    intercept[NoSuchTableException] {
      TiBatchWrite.write(
        df,
        ti,
        new TiDBOptions(
          tidbOptions + ("database" -> s"$dbPrefix$database", "table" -> s"${batchWriteTablePrefix}_TABLE_NOT_EXISTS", "isTest" -> "true")))
    }
  }

  override def afterAll(): Unit =
    try {
      setCurrentDatabase(database)
      for (table <- tables) {
        val tableToWrite = s"${batchWriteTablePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

      for (table <- tables) {
        val tableToWrite = s"${batchWriteTablePrefix}_${isPkHandlePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

      for (table <- tables) {
        val tableToWrite = s"${batchWriteTablePrefix}_${replacePKHandlePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }

      for (table <- tables) {
        val tableToWrite = s"${batchWriteTablePrefix}_${replaceUniquePrefix}_$table"
        tidbStmt.execute(s"drop table if exists `$tableToWrite`")
      }
    } finally {
      super.afterAll()
    }
}
