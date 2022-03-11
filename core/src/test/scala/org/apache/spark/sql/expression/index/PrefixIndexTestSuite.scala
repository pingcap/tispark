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

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.catalyst.util.resourceToString

import java.nio.charset.Charset

class PrefixIndexTestSuite extends BaseTiSparkTest {
  // https://github.com/pingcap/tispark/issues/272
  test("Prefix index read does not work correctly") {
    tidbStmt.execute(
      resourceToString(
        s"prefix-index/PrefixTest.sql",
        classLoader = Thread.currentThread().getContextClassLoader))
    refreshConnections()
    // add explain to show if we have actually used prefix index in plan
    explainAndRunTest("select a, b from prefix where b < \"bbc\"")
    explainAndRunTest("select a, b from prefix where a = 1 and b = \"bbb\"")
    explainAndRunTest("select b from prefix where b = \"bbc\"")
    explainAndRunTest("select b from prefix where b != \"bbc\"")
    explainAndRunTest("select * from prefix where b = 'b'")
    explainAndRunTest("select b from prefix where b >= \"bbc\" and b < \"bbd\"")
    // FIXME: following test results in INDEX range [bb, bb] and TABLE range (-INF, bbc),
    // while the table range should have been [bb, bb]
    // FYI, the predicate is [[b] LESS_THAN "bbc"], Not(IsNull([b])), [[b] EQUAL "bb"]
    explainAndRunTest("select c, b from prefix where b = \"bb\" and b < \"bbc\"")
    println(Charset.defaultCharset())
    explainAndRunTest(
      "select c, b from prefix where b > \"ÿ\" and b < \"ÿÿc\"",
      skipJDBC = true,
      rTiDB = List(List(8, "ÿÿ"), List(9, "ÿÿ0")))
    // add LIKE tests for prefix index
    explainAndRunTest("select a, b from prefix where b LIKE 'b%'")
    explainAndRunTest("select a, b from prefix where b LIKE 'ab%'")
    explainAndRunTest(
      "select a, b from prefix where b LIKE 'ÿÿ%'",
      skipJDBC = true,
      rTiDB = List(List(7, "ÿÿ"), List(8, "ÿÿ0"), List(9, "ÿÿÿ")))
    explainAndRunTest("select a, b from prefix where b LIKE 'b%b'")
    explainAndRunTest("select a, b from prefix where b LIKE 'ÿ%'", skipJDBC = true)
    explainAndRunTest("select a, b from prefix where b LIKE '%b'")
    explainAndRunTest("select a, b from prefix where b LIKE '%'")
  }

  // https://github.com/pingcap/tispark/issues/397
  test("Prefix index implementation for utf8 string is incorrect") {
    tidbStmt.execute(
      resourceToString(
        s"prefix-index/UTF8Test.sql",
        classLoader = Thread.currentThread().getContextClassLoader))
    refreshConnections()

    spark.sql("select * from t1").show
    explainAndRunTest("select * from t1 where name = '中文字符集_测试'", skipJDBC = true)
    explainAndRunTest("select * from t1 where name < '中文字符集_测试'", skipJDBC = true)
    explainAndRunTest("select * from t1 where name > '中文字符集_测试'", skipJDBC = true)
  }

  test("index double scan with predicate") {
    tidbStmt.execute("drop table if exists test_index")
    tidbStmt.execute(
      "create table test_index(id bigint(20), c1 text default null, c2 int, c3 int, c4 int, KEY idx_c1(c1(10)))")
    tidbStmt.execute("insert into test_index values(1, 'aairy', 10, 20, 30)")
    tidbStmt.execute("insert into test_index values(2, 'dairy', 20, 30, 40)")
    tidbStmt.execute("insert into test_index values(3, 'zairy', 30, 40, 50)")
    refreshConnections() // refresh since we need to load data again
    explainAndRunTest("select c1, c2 from test_index where c1 < 'dairy' and c2 > 20")
    explainAndRunTest("select c1, c2 from test_index where c1 = 'dairy'")
    explainAndRunTest("select c1, c2 from test_index where c1 > 'dairy'")
    explainAndRunTest("select c2 from test_index where c1 < 'dairy'")
    explainAndRunTest("select c2 from test_index where c1 = 'dairy'")
    explainAndRunTest("select c2, c2 from test_index where c1 > 'dairy'")
    explainAndRunTest("select c2, c2 from test_index where c1 < 'dairy'")
    explainAndRunTest("select c2, c2 from test_index where c1 = 'dairy'")
    explainAndRunTest("select max(c2) from test_index where c1 > 'dairy'")
    explainAndRunTest("select max(c2) from test_index where c1 < 'dairy'")
    explainAndRunTest("select max(c2) from test_index where c1 = 'dairy'")
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("DROP TABLE IF EXISTS `prefix`")
      tidbStmt.execute("DROP TABLE IF EXISTS `t1`")
      tidbStmt.execute("DROP TABLE IF EXISTS `test_index`")
    } finally {
      super.afterAll()
    }
}
