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

class InTest0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select tp_int from full_data_type_table  where tp_int in (2333, 601508558, 4294967296, 4294967295)",
    "select tp_bigint from full_data_type_table  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)",
    "select tp_varchar from full_data_type_table  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')",
    "select tp_decimal from full_data_type_table  where tp_decimal in (2, 3, 4)",
    "select tp_double from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)",
    "select tp_float from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)",
    "select tp_datetime from full_data_type_table  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')",
    "select tp_date from full_data_type_table  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')",
    "select tp_timestamp from full_data_type_table  where tp_timestamp in ('2017-11-02 16:48:01')",
    "select tp_real from full_data_type_table  where tp_real in (4.44,0.5194052764001038)")

  test("Test - In") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
