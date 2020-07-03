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

class ArithmeticTest2Suite extends BaseInitialOnceTest {
  private val divideCases = Seq[String](
    "select id_dt from full_data_type_table where tp_int / tp_tinyint > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_bigint / tp_int >= 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_bigint / tp_float <= 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_bigint / tp_decimal < 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_int / tp_float != 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_int / tp_double > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_real / tp_double > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_real / tp_smallint > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_real / tp_tinyint > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_bigint / tp_tinyint > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_int / tp_char < 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_double / tp_mediumint > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_int / tp_decimal < 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_float / tp_decimal > 0 order by id_dt",
    "select id_dt from full_data_type_table where tp_double / tp_float > 0 order by id_dt")
  private val multiCases = divideCases map { _.replace("/", "*") }
  private val minusCases = divideCases map { _.replace("/", "-") }
  private val addCases = divideCases map { _.replace("/", "+") }
  private val allCases = addCases ++ minusCases ++ multiCases ++ divideCases

  test("Test - Arithmetic Test2") {
    allCases.foreach { query =>
      runTest(query)
    }
  }
}
