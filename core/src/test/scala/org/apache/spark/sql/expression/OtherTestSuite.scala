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

class OtherTestSuite extends BaseInitialOnceTest {
  private val cases = Seq[String](
    "select id_dt from full_data_type_table where tp_date is null",
    "select id_dt from full_data_type_table where tp_date is not null",
    "select tp_char from full_data_type_table where tp_char like 'g%' order by id_dt",
    "select tp_int from full_data_type_table where tp_int like '-5%' order by id_dt",
    "select tp_float from full_data_type_table where tp_float like '0.51%' order by id_dt",
    "select tp_text from full_data_type_table where tp_text like 'S%' order by id_dt",
    "select tp_char from full_data_type_table where tp_char like '%H%' order by id_dt",
    "select tp_real from full_data_type_table where tp_real like '%44%' order by id_dt",
    "select tp_date from full_data_type_table where tp_date like '2016%' order by id_dt",
    "select tp_date from full_data_type_table where tp_date like '%12%' order by id_dt",
    "select tp_timestamp from full_data_type_table where tp_timestamp like '%00%' order by id_dt")

  test("Test - Other") {
    cases.foreach { query =>
      runTest(query)
    }
  }
}
