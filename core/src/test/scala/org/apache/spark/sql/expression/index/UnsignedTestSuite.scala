/*
 * Copyright 2018 PingCAP, Inc.
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

class UnsignedTestSuite extends BaseTiSparkTest {

  test("Unsigned Index Tests for TISPARK-28 and TISPARK-29") {
    tidbStmt.execute("DROP TABLE IF EXISTS `unsigned_test`")
    tidbStmt.execute("""CREATE TABLE `unsigned_test` (
        |  `c1` bigint(20) UNSIGNED NOT NULL,
        |  `c2` bigint(20) UNSIGNED DEFAULT NULL,
        |  `c3` bigint(20) DEFAULT NULL,
        |  PRIMARY KEY (`c1`),
        |  KEY `idx_c2` (`c2`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin)
    tidbStmt.execute("""INSERT INTO `unsigned_test` VALUES
        |  (1,1,1),
        |  (2,3,4),
        |  (3,5,7),
        |  (0,18446744073709551615,-9223372036854775808),
        |  (18446744073709551615,18446744073709551615,9223372036854775807)""".stripMargin)

    tidbStmt.execute("ANALYZE TABLE `unsigned_test`")
    refreshConnections()

    // TODO: After we fixed unsigned behavior, delete `skipped` setting for this test
    val queries = Seq[String](
      "select * from unsigned_test",
      "select * from unsigned_test where c2 < -1",
      "select * from unsigned_test where c2 > -1",
      "select * from unsigned_test where c2 < 18446744073709551615",
      "select * from unsigned_test where c2 > 18446744073709551615",
      "select * from unsigned_test where c2 <= 18446744073709551615",
      "select * from unsigned_test where c2 < 18446744073709551616",
      "select * from unsigned_test where c2 > 18446744073709551616",
      "select c2 from unsigned_test where c2 < -1",
      "select c2 from unsigned_test where c2 > -1",
      "select c2 from unsigned_test where c2 < 18446744073709551615",
      "select c2 from unsigned_test where c2 > 18446744073709551615",
      "select c2 from unsigned_test where c2 <= 18446744073709551615",
      "select c2 from unsigned_test where c2 < 18446744073709551616",
      "select c2 from unsigned_test where c2 > 18446744073709551616",
      "select c1 from unsigned_test where c2 < 18446744073709551615",
      "select c1 from unsigned_test where c2 <= 18446744073709551615")
    val unsignedLongMaxValue = BigDecimal("18446744073709551615")
    val LongMaxValue = 9223372036854775807L
    val LongMinValue = -9223372036854775808L
    val answers = Seq[List[List[Any]]](
      List(
        List(0, unsignedLongMaxValue, LongMinValue),
        List(1, 1, 1),
        List(2, 3, 4),
        List(3, 5, 7),
        List(unsignedLongMaxValue, unsignedLongMaxValue, LongMaxValue)),
      List.empty,
      List(
        List(0, unsignedLongMaxValue, LongMinValue),
        List(1, 1, 1),
        List(2, 3, 4),
        List(3, 5, 7),
        List(unsignedLongMaxValue, unsignedLongMaxValue, LongMaxValue)),
      List(List(1, 1, 1), List(2, 3, 4), List(3, 5, 7)),
      List.empty,
      List(
        List(0, unsignedLongMaxValue, LongMinValue),
        List(1, 1, 1),
        List(2, 3, 4),
        List(3, 5, 7),
        List(unsignedLongMaxValue, unsignedLongMaxValue, LongMaxValue)),
      List(
        List(0, unsignedLongMaxValue, LongMinValue),
        List(1, 1, 1),
        List(2, 3, 4),
        List(3, 5, 7),
        List(unsignedLongMaxValue, unsignedLongMaxValue, LongMaxValue)),
      List.empty,
      List.empty,
      List(List(unsignedLongMaxValue), List(1), List(3), List(5), List(unsignedLongMaxValue)),
      List(List(1), List(3), List(5)),
      List.empty,
      List(List(unsignedLongMaxValue), List(1), List(3), List(5), List(unsignedLongMaxValue)),
      List(List(unsignedLongMaxValue), List(1), List(3), List(5), List(unsignedLongMaxValue)),
      List.empty,
      List(List(1), List(2), List(3)),
      List(List(0), List(1), List(2), List(3), List(unsignedLongMaxValue)))
    assert(queries.lengthCompare(answers.length) == 0)
    for (i <- queries.indices) {
      println(queries(i))
      explainAndRunTest(queries(i), rJDBC = answers(i), skipTiDB = true)
    }
  }

  override def afterAll(): Unit =
    try {
      tidbStmt.execute("drop table if exists `unsigned_test`")
    } finally {
      super.afterAll()
    }
}
