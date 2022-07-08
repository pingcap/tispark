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
 * support type (MYSQLTYPE):smallint,bigint,decimal,mediumint,real(double),tinyint,int,double,year

 * unsupported type: char,float,datatime,varchar,timestamp
 * because Spark cast them to double,cast can't be pushed down to tikv
 *
 * This test will
 * 1. check whether sum is pushed down
 * 2. check whether the result is right(equals to spark jdbc or equals to tidb)
 */
class SumPushDownSuite extends BasePushDownSuite {

  private val allCases = Seq[String](
    "select sum(tp_smallint) from ",
    "select sum(tp_bigint) from ",
    "select sum(tp_decimal) from ",
    "select sum(tp_mediumint) from ",
    "select sum(tp_real) from ",
    "select sum(tp_tinyint) from ",
    "select sum(id_dt) from ",
    "select sum(tp_int) from ",
    "select sum(tp_double) from ")

  test("Test - Sum push down with pk") {
    val tableName = "full_data_type_table_pk"
    allCases.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"sum is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }
  }

  test("Test - Sum push down no pk") {
    val tableName = "full_data_type_table_no_pk"
    allCases.foreach { query =>
      val sql = query + tableName
      val df = spark.sql(sql)
      if (!extractCoprocessorRDDs(df).head.toString.contains("Aggregates")) {
        fail(
          s"sum is not pushed down in query:$query,DAGRequests:" + extractCoprocessorRDDs(
            df).head.toString)
      }
      runTest(sql)
    }
  }

}
