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

class Between0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select tp_int from full_data_type_table_idx  where tp_int between -1202333 and 601508558 order by id_dt ",
    "select tp_bigint from full_data_type_table_idx  where tp_bigint between -2902580959275580308 and 9223372036854775807 order by id_dt ",
    "select tp_decimal from full_data_type_table_idx  where tp_decimal between 2 and 200 order by id_dt ",
    "select tp_double from full_data_type_table_idx  where tp_double between 0.2054466 and 3.1415926 order by id_dt ",
    "select tp_float from full_data_type_table_idx  where tp_double between -313.1415926 and 30.9412022 order by id_dt ",
    "select tp_datetime from full_data_type_table_idx  where tp_datetime between '2043-11-28 00:00:00' and '2017-09-07 11:11:11' order by id_dt ",
    "select tp_date from full_data_type_table_idx  where tp_date between '2017-11-02' and '2043-11-28' order by id_dt ",
    "select tp_real from full_data_type_table_idx  where tp_real between 4.44 and 0.5194052764001038 order by id_dt",
    "select tp_real from full_data_type_table_idx  where tp_real between 0.5194052764001038 and 4.44 order by id_dt ")

  test("Test index Between") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
