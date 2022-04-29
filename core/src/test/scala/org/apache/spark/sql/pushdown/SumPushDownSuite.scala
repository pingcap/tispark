/*
 *
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
 *
 */

package org.apache.spark.sql.pushdown

import org.apache.spark.sql.catalyst.plans.BasePlanTest

/**
 * support type (MYSQLTYPE):smallint,bigint,decimal,mediumint,real(double),tinyint,int,double

 * unsupported type: char,float,datatime,varchar,timestamp
 * because Spark cast them to double,cast can't be pushed down to tikv
 *
 * This test will
 * 1. check whether sum is pushed down
 * 2. check whether the result is right(equals to spark jdbc or equals to tidb)
 */
class SumPushDownSuite extends BasePlanTest {

  private val allCases = Seq[String](
    "select sum(tp_smallint) from full_data_type_table",
    "select sum(tp_bigint) from full_data_type_table",
    "select sum(tp_decimal) from full_data_type_table",
    "select sum(tp_mediumint) from full_data_type_table",
    "select sum(tp_real) from full_data_type_table",
    "select sum(tp_tinyint) from full_data_type_table",
    "select sum(id_dt) from full_data_type_table",
    "select sum(tp_int) from full_data_type_table",
    "select sum(tp_double) from full_data_type_table")

  test("Test - Sum push down") {
    allCases.foreach { query =>
      val df = spark.sql(query)
      if (!extractDAGRequests(df).head.toString.contains("Aggregates")) {
        df.explain()
        fail(s"sum is not pushed down in query:$query")
      }
      runTest(query)
    }
  }

}
