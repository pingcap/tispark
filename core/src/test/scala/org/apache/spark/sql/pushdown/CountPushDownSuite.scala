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

import org.tikv.common.StoreVersion

/**
 * Count will be pushed down except:
 * - Count(col1,col2,...) can't be pushed down (TiDB not support)
 * - Count(set) can't be pushed down
 */
class CountPushDownSuite extends BasePushDownSuite() {

  private val push = Seq[String](
    "select count(id_dt) from ",
    "select count(tp_varchar) from ",
    "select count(tp_datetime) from ",
    "select count(tp_blob) from ",
    "select count(tp_binary) from ",
    "select count(tp_date) from ",
    "select count(tp_timestamp) from ",
    "select count(tp_year) from ",
    "select count(tp_bigint) from ",
    "select count(tp_decimal) from ",
    "select count(tp_double) from ",
    "select count(tp_float) from ",
    "select count(tp_int) from ",
    "select count(tp_mediumint) from ",
    "select count(tp_real) from ",
    "select count(tp_smallint) from ",
    "select count(tp_tinyint) from ",
    "select count(tp_char) from ",
    "select count(tp_nvarchar) from ",
    "select count(tp_longtext) from ",
    "select count(tp_mediumtext) from ",
    "select count(tp_text) from ",
    "select count(tp_tinytext) from ",
    "select count(tp_time) from ",
    "select count(*) from ",
    "select count(1) from ")

  private val notPush = Seq[String]("select count(tp_set) from ")

  test("Test - Count push down with pk") {
    val tableName = "full_data_type_table_pk"
    push.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }

    notPush.foreach { query =>
      val sql = query + tableName
      runTest(sql)
    }

    // Count(bit) can push down after 6.0.0
    if (StoreVersion.minTiKVVersion("6.0.0", ti.clientSession.getTikvSession.getPDClient)) {
      val query = "select count(tp_bit) from " + tableName
      val df = spark.sql(query)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(query)
    }

    // Count(enum) can push down after 5.1.0
    if (StoreVersion.minTiKVVersion("5.1.0", ti.clientSession.getTikvSession.getPDClient)) {
      val query = "select count(tp_enum) from " + tableName
      val df = spark.sql(query)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(query)
    }

  }

  test("Test - Count push down no pk") {
    val tableName = "full_data_type_table_no_pk"
    push.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }

    notPush.foreach { query =>
      val sql = query + tableName
      runTest(sql)
    }

    // Count(bit) can push down after 6.0.0
    if (StoreVersion.minTiKVVersion("6.0.0", ti.clientSession.getTikvSession.getPDClient)) {
      val query = "select count(tp_bit) from " + tableName
      val df = spark.sql(query)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(query)
    }

    // Count(enum) can push down after 5.1.0
    if (StoreVersion.minTiKVVersion("5.1.0", ti.clientSession.getTikvSession.getPDClient)) {
      val query = "select count(tp_enum) from " + tableName
      val df = spark.sql(query)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"count is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(query)
    }

  }
}
