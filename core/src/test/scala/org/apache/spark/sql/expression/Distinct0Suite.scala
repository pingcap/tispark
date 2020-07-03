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

class Distinct0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select  distinct(tp_binary)  from full_data_type_table  order by tp_binary ",
    "select  distinct(tp_nvarchar)  from full_data_type_table  order by tp_nvarchar ",
    "select  distinct(tp_date)  from full_data_type_table  order by tp_date ",
    "select  distinct(tp_tinyint)  from full_data_type_table  order by tp_tinyint ",
    "select  distinct(tp_int)  from full_data_type_table  order by tp_int ",
    "select  distinct(tp_mediumint)  from full_data_type_table  order by tp_mediumint ",
    "select  distinct(tp_real)  from full_data_type_table  order by tp_real ",
    "select  distinct(tp_blob)  from full_data_type_table  order by tp_blob ",
    "select  distinct(tp_datetime)  from full_data_type_table  order by tp_datetime ",
    "select  distinct(tp_varchar)  from full_data_type_table  order by tp_varchar ",
    "select  distinct(id_dt)  from full_data_type_table  order by id_dt ",
    "select  distinct(tp_float)  from full_data_type_table  order by tp_float ",
    "select  distinct(tp_char)  from full_data_type_table  order by tp_char ",
    "select  distinct(tp_decimal)  from full_data_type_table  order by tp_decimal ",
    "select  distinct(tp_timestamp)  from full_data_type_table  order by tp_timestamp ",
    "select  distinct(tp_bigint)  from full_data_type_table  order by tp_bigint ",
    "select  distinct(tp_smallint)  from full_data_type_table  order by tp_smallint ",
    "select  distinct(tp_double)  from full_data_type_table  order by tp_double ")

  test("Test - Distinct") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
