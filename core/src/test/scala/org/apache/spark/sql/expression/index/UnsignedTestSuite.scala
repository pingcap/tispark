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

import org.apache.spark.sql.BaseTiSparkSuite

class UnsignedTestSuite extends BaseTiSparkSuite {

  test("Unsigned Index Tests") {
    tidbStmt.execute("DROP TABLE IF EXISTS `unsigned_test`")
    tidbStmt.execute(
      """CREATE TABLE `unsigned_test` (
        |  `c1` bigint(20) UNSIGNED NOT NULL,
        |  `c2` bigint(20) UNSIGNED DEFAULT NULL,
        |  `c3` bigint(20) DEFAULT NULL,
        |  PRIMARY KEY (`c1`),
        |  KEY `idx_c2` (`c2`)
        |) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin""".stripMargin
    )
    tidbStmt.execute(
      "insert into `unsigned_test` values(1,1,1),(2,3,4),(3,5,7),(9223372036854775807,9223372036854775807,0)"
    )
    tidbStmt.execute("ANALYZE TABLE `unsigned_test`")
    refreshConnections()
    explainAndRunTest("select * from unsigned_test where c2 < -1", skipTiDB = true, skipped = true)
    explainAndRunTest("select c2 from unsigned_test where c2 < -1", skipTiDB = true, skipped = true)
  }

  override def afterAll(): Unit = {
    try {
      tidbStmt.execute("drop table if exists `unsigned_test`")
    } finally {
      super.afterAll()
    }
  }
}
