/*
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
 */

package org.apache.spark.sql.expression.index

import org.apache.spark.sql.BaseTiSparkSuite

class PrefixIndexTestSuite extends BaseTiSparkSuite {
  // https://github.com/pingcap/tispark/issues/272
  test("Prefix index read does not work correctly") {
    tidbStmt.execute("DROP TABLE IF EXISTS `prefix`")
    tidbStmt.execute(
      "CREATE TABLE `prefix` (\n  `a` int(11) NOT NULL,\n  `b` varchar(55) DEFAULT NULL,\n  `c` int(11) DEFAULT NULL,\n  PRIMARY KEY (`a`),\n  KEY `prefix_index` (`b`(2)),\n KEY `prefix_complex` (`a`, `b`(2))\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
    )
    tidbStmt.execute(
      "INSERT INTO `prefix` VALUES(0, \"b\", 2), (1, \"bbb\", 3), (2, \"bbc\", 4), (3, \"bbb\", 5), (4, \"abc\", 6), (5, \"abc\", 7), (6, \"abc\", 7), (7, \"ÿÿ\", 8), (8, \"ÿÿ0\", 9), (9, \"ÿÿÿ\", 10)"
    )
    println(
      "INSERT INTO `prefix` VALUES(0, \"b\", 2), (1, \"bbb\", 3), (2, \"bbc\", 4), (3, \"bbb\", 5), (4, \"abc\", 6), (5, \"abc\", 7), (6, \"abc\", 7), (7, \"ÿÿ\", 8), (8, \"ÿÿ0\", 9), (9, \"ÿÿÿ\", 10)"
    )
    tidbStmt.execute("ANALYZE TABLE `prefix`")
    refreshConnections()
    // add explain to show if we have actually used prefix index in plan
    explainAndRunTest("select a, b from prefix where b < \"bbc\"")
    explainAndRunTest("select a, b from prefix where a = 1 and b = \"bbb\"")
    explainAndRunTest("select b from prefix where b = \"bbc\"")
    explainAndRunTest("select b from prefix where b != \"bbc\"")
    explainAndRunTest("select b from prefix where b >= \"bbc\" and b < \"bbd\"")
    // FIXME: following test results in INDEX range [bb, bb] and TABLE range (-INF, bbc), while the table range should have been [bb, bb]
    // FYI, the predicate is [[b] LESS_THAN "bbc"], Not(IsNull([b])), [[b] EQUAL "bb"]
    explainAndRunTest("select c, b from prefix where b = \"bb\" and b < \"bbc\"")
    explainAndRunTest(
      "select c, b from prefix where b > \"ÿ\" and b < \"ÿÿc\"",
      skipJDBC = true,
      rTiDB = List(List(8, "ÿÿ"), List(9, "ÿÿ0"))
    )
    // add LIKE tests for prefix index
    explainAndRunTest("select a, b from prefix where b LIKE 'b%'")
    explainAndRunTest("select a, b from prefix where b LIKE 'ab%'")
    explainAndRunTest(
      "select a, b from prefix where b LIKE 'ÿÿ%'",
      skipJDBC = true,
      rTiDB = List(List(7, "ÿÿ"), List(8, "ÿÿ0"), List(9, "ÿÿÿ"))
    )
    explainAndRunTest("select a, b from prefix where b LIKE 'b%b'")
    explainAndRunTest("select a, b from prefix where b LIKE 'ÿ%'", skipJDBC = true)
    explainAndRunTest("select a, b from prefix where b LIKE '%b'")
    explainAndRunTest("select a, b from prefix where b LIKE '%'")
  }

  // https://github.com/pingcap/tispark/issues/397
  test("Prefix index implementation for utf8 string is incorrect") {
    tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    tidbStmt.execute("""CREATE TABLE `t1` (
                       |  `name` varchar(12) DEFAULT NULL,
                       |  KEY `pname` (`name`(12))
                       |) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
      """.stripMargin)
    tidbStmt.execute("insert into t1 values('借款策略集_网页')")
    refreshConnections()

    println("insert into t1 values('借款策略集_网页')")
    spark.sql("select * from t1").show
    println(tidbStmt.executeQuery("select * from t1"))
    spark.sql("select * from t1 where name = '借款策略集_网页'").explain
    spark.sql("select * from t1 where name = '借款策略集_网页'").show
    spark.sql("select * from t1 where name < '借款策略集_网页'").show
    runTest("select * from t1 where name = '借款策略集_网页'", skipJDBC = true)
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("DROP TABLE IF EXISTS `prefix`")
      tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
    } finally {
      super.afterAll()
    }
}
