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

class Join0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_decimal = b.tp_decimal",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.id_dt = b.id_dt",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_double = b.tp_double",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_real = b.tp_real",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_mediumint = b.tp_mediumint",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_bigint = b.tp_bigint",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_datetime = b.tp_datetime",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_smallint = b.tp_smallint",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_float = b.tp_float",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_int = b.tp_int",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_tinyint = b.tp_tinyint",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_date = b.tp_date",
    "select a.id_dt from full_data_type_table_idx a join full_data_type_table_idx b on a.tp_timestamp = b.tp_timestamp")

  test("Test index - Join") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
