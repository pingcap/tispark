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

class Aggregate0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select tp_mediumtext from full_data_type_table  group by (tp_mediumtext)  order by tp_mediumtext ",
    "select tp_double from full_data_type_table  group by (tp_double)  order by tp_double ",
    "select tp_smallint from full_data_type_table  group by (tp_smallint)  order by tp_smallint ",
    "select tp_nvarchar from full_data_type_table  group by (tp_nvarchar)  order by tp_nvarchar ",
    "select tp_real from full_data_type_table  group by (tp_real)  order by tp_real ",
    "select tp_binary from full_data_type_table  group by (tp_binary)  order by tp_binary ",
    "select tp_text from full_data_type_table  group by (tp_text)  order by tp_text ",
    "select tp_blob from full_data_type_table  group by (tp_blob)  order by tp_blob ",
    "select tp_date from full_data_type_table  group by (tp_date)  order by tp_date ",
    "select id_dt from full_data_type_table  group by (id_dt)  order by id_dt ",
    "select tp_mediumint from full_data_type_table  group by (tp_mediumint)  order by tp_mediumint ",
    "select tp_tinyint from full_data_type_table  group by (tp_tinyint)  order by tp_tinyint ",
    "select tp_tinytext from full_data_type_table  group by (tp_tinytext)  order by tp_tinytext ",
    "select tp_float from full_data_type_table  group by (tp_float)  order by tp_float ",
    "select tp_bigint from full_data_type_table  group by (tp_bigint)  order by tp_bigint ",
    "select tp_int from full_data_type_table  group by (tp_int)  order by tp_int ",
    "select tp_timestamp from full_data_type_table  group by (tp_timestamp)  order by tp_timestamp ",
    "select tp_decimal from full_data_type_table  group by (tp_decimal)  order by tp_decimal ",
    "select tp_char from full_data_type_table  group by (tp_char)  order by tp_char ",
    "select tp_longtext from full_data_type_table  group by (tp_longtext)  order by tp_longtext ",
    "select tp_varchar from full_data_type_table  group by (tp_varchar)  order by tp_varchar ",
    "select tp_datetime from full_data_type_table  group by (tp_datetime)  order by tp_datetime ",
    "select 999 + tp_int + sum(tp_int + 999) from full_data_type_table  group by tp_int + 999 order by 1",
    "select 999 + tp_int + sum(tp_int) from full_data_type_table  group by tp_int + 999 order by 1",
    "select 999 + tp_int+sum(tp_int), tp_int + 999 + 1 from full_data_type_table  group by tp_int + 999 order by 1,2",
    "select tp_int + 999+1 from full_data_type_table group by tp_int + 999 order by 1")

  test("Test - Aggregate") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
