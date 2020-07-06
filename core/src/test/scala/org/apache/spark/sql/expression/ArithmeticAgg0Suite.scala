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

class ArithmeticAgg0Suite extends BaseInitialOnceTest {
  private val allCases = Seq[String](
    "select min(tp_smallint) from full_data_type_table",
    "select sum(tp_char) from full_data_type_table",
    "select min(tp_tinyint) from full_data_type_table",
    "select avg(tp_int) from full_data_type_table",
    "select avg(tp_timestamp) from full_data_type_table",
    "select max(tp_bigint) from full_data_type_table",
    "select avg(tp_datetime) from full_data_type_table",
    "select avg(tp_double) from full_data_type_table",
    "select sum(tp_smallint) from full_data_type_table",
    "select sum(tp_float) from full_data_type_table",
    "select avg(tp_char) from full_data_type_table",
    "select avg(tp_bigint) from full_data_type_table",
    "select sum(tp_bigint) from full_data_type_table",
    "select sum(tp_decimal) from full_data_type_table",
    "select sum(tp_mediumint) from full_data_type_table",
    "select abs(tp_float) from full_data_type_table",
    "select max(tp_tinytext) from full_data_type_table",
    "select min(tp_date) from full_data_type_table",
    "select min(tp_real) from full_data_type_table",
    "select sum(tp_real) from full_data_type_table",
    "select min(tp_longtext) from full_data_type_table",
    "select max(tp_int) from full_data_type_table",
    "select avg(tp_mediumint) from full_data_type_table",
    "select sum(tp_tinyint) from full_data_type_table",
    "select max(tp_nvarchar) from full_data_type_table",
    "select max(tp_blob) from full_data_type_table",
    "select abs(tp_real) from full_data_type_table",
    "select sum(id_dt) from full_data_type_table",
    "select sum(tp_datetime) from full_data_type_table",
    "select avg(tp_nvarchar) from full_data_type_table",
    "select min(tp_varchar) from full_data_type_table",
    "select avg(tp_real) from full_data_type_table",
    "select avg(tp_decimal) from full_data_type_table",
    "select abs(tp_mediumint) from full_data_type_table",
    "select min(tp_datetime) from full_data_type_table",
    "select max(tp_timestamp) from full_data_type_table",
    "select max(tp_char) from full_data_type_table",
    "select max(tp_mediumtext) from full_data_type_table",
    "select min(tp_timestamp) from full_data_type_table",
    "select abs(tp_double) from full_data_type_table",
    "select max(tp_decimal) from full_data_type_table",
    "select min(tp_text) from full_data_type_table",
    "select avg(tp_tinyint) from full_data_type_table",
    "select abs(tp_tinyint) from full_data_type_table",
    "select min(tp_float) from full_data_type_table",
    "select max(tp_binary) from full_data_type_table",
    "select max(tp_float) from full_data_type_table",
    "select sum(tp_int) from full_data_type_table",
    "select max(tp_smallint) from full_data_type_table",
    "select max(tp_tinyint) from full_data_type_table",
    "select avg(id_dt) from full_data_type_table",
    "select abs(tp_decimal) from full_data_type_table",
    "select abs(id_dt) from full_data_type_table",
    "select min(tp_binary) from full_data_type_table",
    "select min(tp_tinytext) from full_data_type_table",
    "select sum(tp_varchar) from full_data_type_table",
    "select min(tp_mediumtext) from full_data_type_table",
    "select avg(tp_smallint) from full_data_type_table",
    "select min(id_dt) from full_data_type_table",
    "select max(tp_longtext) from full_data_type_table",
    "select min(tp_double) from full_data_type_table",
    "select max(tp_date) from full_data_type_table",
    "select max(tp_real) from full_data_type_table",
    "select max(tp_mediumint) from full_data_type_table",
    "select abs(tp_int) from full_data_type_table",
    "select min(tp_mediumint) from full_data_type_table",
    "select abs(tp_bigint) from full_data_type_table",
    "select max(id_dt) from full_data_type_table",
    "select abs(tp_smallint) from full_data_type_table",
    "select max(tp_double) from full_data_type_table",
    "select max(tp_datetime) from full_data_type_table",
    "select avg(tp_varchar) from full_data_type_table",
    "select sum(tp_nvarchar) from full_data_type_table",
    "select min(tp_blob) from full_data_type_table",
    "select avg(tp_float) from full_data_type_table",
    "select sum(tp_double) from full_data_type_table",
    "select min(tp_decimal) from full_data_type_table",
    "select max(tp_varchar) from full_data_type_table",
    "select min(tp_nvarchar) from full_data_type_table",
    "select sum(tp_timestamp) from full_data_type_table",
    "select max(tp_text) from full_data_type_table",
    "select min(tp_bigint) from full_data_type_table",
    "select min(tp_int) from full_data_type_table",
    "select min(tp_char) from full_data_type_table")

  test("Test - ArithmeticAgg") {
    allCases.foreach { query =>
      runTest(query)
    }
  }

}
