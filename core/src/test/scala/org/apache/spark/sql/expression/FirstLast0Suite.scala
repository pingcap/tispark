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

class FirstLast0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select first(tp_char) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_double) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_double) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_decimal) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_bigint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_bigint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_date) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_varchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_tinytext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_float) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_mediumtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_int) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(id_dt) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_real) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_datetime) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_smallint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_tinyint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_tinytext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_longtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_blob) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_text) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_date) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_mediumint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_datetime) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_smallint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_mediumint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_longtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_float) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_binary) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_binary) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_blob) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_nvarchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_text) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_char) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(id_dt) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_decimal) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_varchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_nvarchar) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_tinyint) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_int) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select first(tp_real) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ",
    "select last(tp_mediumtext) from full_data_type_table  group by (tp_nvarchar)   order by tp_nvarchar ")

  test("Test - First/Last") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
