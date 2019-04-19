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

import java.nio.charset.Charset

import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.catalyst.util.resourceToString

class PrefixIndexTestSuite extends BaseTiSparkSuite {
  // https://github.com/pingcap/tispark/issues/272
  test("Prefix index read does not work correctly") {
    tidbStmt.execute(
      resourceToString(
        s"prefix-index/PrefixTest.sql",
        classLoader = Thread.currentThread().getContextClassLoader
      )
    )
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
    println(Charset.defaultCharset())
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
    explainAndRunTest("select * from prefix where b = 'b'")
  }

  // https://github.com/pingcap/tispark/issues/397
  test("Prefix index implementation for utf8 string is incorrect") {
    tidbStmt.execute(
      resourceToString(
        s"prefix-index/UTF8Test.sql",
        classLoader = Thread.currentThread().getContextClassLoader
      )
    )
    refreshConnections()

    spark.sql("select * from t1").show
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
