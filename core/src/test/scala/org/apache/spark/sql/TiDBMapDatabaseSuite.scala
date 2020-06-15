/*
 *
 * Copyright 2017 PingCAP, Inc.
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
 *
 */

package org.apache.spark.sql

class TiDBMapDatabaseSuite extends BaseTiSparkTest {

  test("TiDBMapDatabase fails when Decimal length exceeding 38") {
    tidbStmt.execute("drop database if exists decimals")
    tidbStmt.execute("create database decimals")
    tidbStmt.execute("use decimals")
    tidbStmt.execute("drop table if exists high_decimal_precision")
    tidbStmt.execute(
      "CREATE TABLE `high_decimal_precision` (\n  `a` decimal(50) DEFAULT NULL,\n  `b` decimal(22,10) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin")
    tidbStmt.execute(
      "insert into high_decimal_precision values(123456789012345678901234567890123456789012345678, 12.31411, 4), (223456789012345678901234567890123456789012345678, 123131414141.31431311, 6);")
    refreshConnections(TestTables("decimals", "high_decimal_precision"))
    judge("select b from high_decimal_precision")
    spark.sql("select a from high_decimal_precision").show(false)
    spark.sql("select a, b from high_decimal_precision").show(false)
  }

  test("tidbMapDatabase fails") {
    tidbStmt.execute("drop database if exists `test-a`")
    tidbStmt.execute("create database `test-a`")
    tidbStmt.execute("use `test-a`")
    tidbStmt.execute("drop table if exists `t-a`")
    tidbStmt.execute("create table `t-a`(`c-a` int default 1)")
    tidbStmt.execute("insert into `t-a` values(1), (2), (3)")
    refreshConnections(TestTables("test-a", "t-a"))
    judge("select * from `t-a`")
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop database if exists `test-a`")
      tidbStmt.execute("drop database if exists `decimals`")
      refreshConnections()
    } finally {
      super.afterAll()
    }
}
