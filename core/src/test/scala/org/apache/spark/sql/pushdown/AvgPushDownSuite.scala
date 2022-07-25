/*
 * Copyright 2022 PingCAP, Inc.
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
 */

package org.apache.spark.sql.pushdown

/**
 * AVG only push down when count and sum can push down
 * year type is not tested, because spark jdbc not support
 */
class AvgPushDownSuite extends BasePushDownSuite() {

  private val push = Seq[String](
    "select avg(id_dt) from ",
    "select avg(tp_bigint) from ",
    "select avg(tp_decimal) from ",
    "select avg(tp_double) from ",
    "select avg(tp_int) from ",
    "select avg(tp_mediumint) from ",
    "select avg(tp_real) from ",
    "select avg(tp_smallint) from ",
    "select avg(tp_tinyint) from ")

  private val notPush = Seq[String]("select avg(tp_float) from ")

  test("Test - Ave push down pk") {
    val tableName = "full_data_type_table_pk"
    push.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"avg is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }

    notPush.foreach { query =>
      val sql = query + tableName
      runTest(sql)
    }
  }

  test("Test - Avg push down no pk") {
    val tableName = "full_data_type_table_no_pk"
    push.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"avg is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }

    notPush.foreach { query =>
      val sql = query + tableName
      runTest(sql)
    }
  }
}
