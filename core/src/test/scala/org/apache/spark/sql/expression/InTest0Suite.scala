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

import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.test.SharedSQLContext

class InTest0Suite
  extends BaseTiSparkSuite
  with SharedSQLContext {
           

  test("select tp_int from full_data_type_table  where tp_int in (2333, 601508558, 4294967296, 4294967295)") {
    runTest("select tp_int from full_data_type_table  where tp_int in (2333, 601508558, 4294967296, 4294967295)",
            "select tp_int from full_data_type_table_j  where tp_int in (2333, 601508558, 4294967296, 4294967295)")
  }
           

  test("select tp_bigint from full_data_type_table  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)") {
    runTest("select tp_bigint from full_data_type_table  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)",
            "select tp_bigint from full_data_type_table_j  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808)")
  }
           

  test("select tp_varchar from full_data_type_table  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')") {
    runTest("select tp_varchar from full_data_type_table  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')",
            "select tp_varchar from full_data_type_table_j  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef')")
  }
           

  test("select tp_decimal from full_data_type_table  where tp_decimal in (2, 3, 4)") {
    runTest("select tp_decimal from full_data_type_table  where tp_decimal in (2, 3, 4)",
            "select tp_decimal from full_data_type_table_j  where tp_decimal in (2, 3, 4)")
  }
           

  test("select tp_double from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)") {
    runTest("select tp_double from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)",
            "select tp_double from full_data_type_table_j  where tp_double in (0.2054466,3.1415926,0.9412022)")
  }
           

  test("select tp_float from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)") {
    runTest("select tp_float from full_data_type_table  where tp_double in (0.2054466,3.1415926,0.9412022)",
            "select tp_float from full_data_type_table_j  where tp_double in (0.2054466,3.1415926,0.9412022)")
  }
           

  test("select tp_datetime from full_data_type_table  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')") {
    runTest("select tp_datetime from full_data_type_table  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')",
            "select tp_datetime from full_data_type_table_j  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00')")
  }
           

  test("select tp_date from full_data_type_table  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')") {
    runTest("select tp_date from full_data_type_table  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')",
            "select tp_date from full_data_type_table_j  where tp_date in ('2017-11-02', '2043-11-28 00:00:00')")
  }
           

  test("select tp_timestamp from full_data_type_table  where tp_timestamp in ('2017-11-02 16:48:01')") {
    runTest("select tp_timestamp from full_data_type_table  where tp_timestamp in ('2017-11-02 16:48:01')",
            "select tp_timestamp from full_data_type_table_j  where tp_timestamp in ('2017-11-02 16:48:01')")
  }
           

  test("select tp_real from full_data_type_table  where tp_real in (4.44,0.5194052764001038)") {
    runTest("select tp_real from full_data_type_table  where tp_real in (4.44,0.5194052764001038)",
            "select tp_real from full_data_type_table_j  where tp_real in (4.44,0.5194052764001038)")
  }
           
}