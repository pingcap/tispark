/*
 *
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
 *
 */

package org.apache.spark.sql

import org.apache.spark.sql.test.SharedSQLContext

class Having0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select tp_int%1000 a, count(*) from full_data_type_table group by (tp_int%1000) having sum(tp_int%1000) > 100 order by a") {
    val r1 = querySpark("select tp_int%1000 a, count(*) from full_data_type_table group by (tp_int%1000) having sum(tp_int%1000) > 100 order by a")
    val r2 = querySpark("select tp_int%1000 a, count(*) from full_data_type_table_j group by (tp_int%1000) having sum(tp_int%1000) > 100 order by a")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           

  test("select tp_bigint%1000 a, count(*) from full_data_type_table group by (tp_bigint%1000) having sum(tp_bigint%1000) < 100 order by a") {
    val r1 = querySpark("select tp_bigint%1000 a, count(*) from full_data_type_table group by (tp_bigint%1000) having sum(tp_bigint%1000) < 100 order by a")
    val r2 = querySpark("select tp_bigint%1000 a, count(*) from full_data_type_table_j group by (tp_bigint%1000) having sum(tp_bigint%1000) < 100 order by a")
    val result = compResult(r1, r2)
    if (!result) {
      fail(s"Failed with \nTiSpark:\t\t$r1\nSpark With JDBC:$r2")
    }
  }
           
}