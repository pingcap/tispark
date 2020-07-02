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

package org.apache.spark.sql.expression

import org.apache.spark.sql.BaseInitialOnceTest

class ComplexGroupBySuite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select tp_int + 1 from full_data_type_table  group by (tp_int + 1)",
    "select tp_float * 2 from full_data_type_table  group by (tp_float * 2)",
    // group by on floating type is dangerous, we need further discussion.
//  "select tp_float - 2 from full_data_type_table  group by (tp_float - 2)",
    "select tp_float / 2 from full_data_type_table  group by (tp_float / 2)",
//    "select tp_int + tp_int from full_data_type_table group by (tp_int + tp_int)",
    "select tp_int + tp_bigint from full_data_type_table group by (tp_int + tp_bigint)",
    "select tp_float + tp_float from full_data_type_table group by (tp_float + tp_float)",
    "select tp_real + tp_float from full_data_type_table group by (tp_real + tp_float)",
    "select tp_decimal + tp_float from full_data_type_table group by (tp_decimal + tp_float)",
    "select tp_int + tp_float from full_data_type_table group by (tp_int + tp_float)",
    "select tp_int + tp_float - tp_double / 5 + tp_bigint / tp_int from full_data_type_table group by (tp_int + tp_float - tp_double / 5 + tp_bigint / tp_int)")

  test("Test - Complex GroupBy") {
    allCases.foreach { query =>
      runTest(query)
    }
  }
}
