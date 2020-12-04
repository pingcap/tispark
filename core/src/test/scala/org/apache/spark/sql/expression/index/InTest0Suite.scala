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

package org.apache.spark.sql.expression.index

import org.apache.spark.sql.BaseInitialOnceTest

class InTest0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select tp_int from full_data_type_table_idx  where tp_int in (2333, 601508558, 4294967296, 4294967295) order by id_dt ",
    "select tp_bigint from full_data_type_table_idx  where tp_bigint in (122222, -2902580959275580308, 9223372036854775807, 9223372036854775808) order by id_dt ",
    "select tp_varchar from full_data_type_table_idx  where tp_varchar in ('nova', 'a948ddcf-9053-4700-916c-983d4af895ef') order by id_dt ",
    "select tp_decimal from full_data_type_table_idx  where tp_decimal in (2, 3, 4) order by id_dt ",
    "select tp_double from full_data_type_table_idx  where tp_double in (0.2054466,3.1415926,0.9412022) order by id_dt ",
    "select tp_float from full_data_type_table_idx  where tp_double in (0.2054466,3.1415926,0.9412022) order by id_dt ",
    "select tp_datetime from full_data_type_table_idx  where tp_datetime in ('2043-11-28 00:00:00','2017-09-07 11:11:11','1986-02-03 00:00:00') order by id_dt ",
    "select tp_date from full_data_type_table_idx  where tp_date in ('2017-11-02', '2043-11-28 00:00:00') order by id_dt ",
    "select tp_timestamp from full_data_type_table_idx  where tp_timestamp in ('2017-11-02 16:48:01') order by id_dt ",
    "select tp_real from full_data_type_table_idx  where tp_real in (4.44,0.5194052764001038) order by id_dt ")

  test("Test index In") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
