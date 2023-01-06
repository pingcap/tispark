/*
 *
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
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.spark.sql.statistics

import org.apache.spark.sql.BaseTiSparkTest
import org.apache.spark.sql.execution.SimpleMode
import org.scalatest.Matchers.{convertToAnyShouldWrapper, include}

class StatisticsManagerSuite extends BaseTiSparkTest {

  // fix issue: https://github.com/pingcap/tispark/issues/2573
  test("Physical Plan should print EstimatedCount") {
    tidbStmt.execute("analyze table tpch_test.LINEITEM")
    val df = spark
      .sql("""select * from tidb_catalog.tpch_test.LINEITEM
          |""".stripMargin)
      .groupBy("l_linestatus")
      .count()

    val explainStr = df.queryExecution.explainString(SimpleMode)

    explainStr should include("EstimatedCount:")
  }
}
