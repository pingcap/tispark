/*
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
 */

package org.apache.spark.sql

class SparkDataTypeTestSuite extends BaseTiSparkTest {

  test("integer type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_bigint, tp_int, tp_mediumint, tp_smallint, tp_tinyint from full_data_type_table order by tp_int desc nulls first limit 10",
      qTiDB =
        "select id_dt, tp_bigint, tp_int, tp_mediumint, tp_smallint, tp_tinyint from full_data_type_table order by tp_int is null desc, tp_int desc limit 10")
  }

  test("double type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_double, tp_float, tp_real from full_data_type_table order by tp_double desc nulls first limit 10",
      qTiDB =
        "select id_dt, tp_double, tp_float, tp_real from full_data_type_table order by tp_double is null desc, tp_double desc limit 10")
  }

  test("decimal type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_decimal from full_data_type_table order by tp_decimal desc nulls first limit 10",
      qTiDB =
        "select id_dt, tp_decimal from full_data_type_table order by tp_decimal is null desc, tp_decimal desc limit 10")
  }

  test("date type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_date from full_data_type_table order by tp_date desc nulls first limit 10",
      qTiDB =
        "select id_dt, tp_date from full_data_type_table order by tp_date is null desc, tp_date desc limit 10")
  }

  test("datetime type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_datetime, tp_date, tp_timestamp from full_data_type_table order by tp_date desc nulls first limit 10",
      qTiDB =
        "select id_dt, tp_datetime, tp_date, tp_timestamp from full_data_type_table order by tp_date is null desc, tp_date desc limit 10")
  }

  test("time/year type test") {
    val result = List(List(2004, null, null), List(-1000, 2017, 60203000000000L))
    checkSparkResult(
      "select id_dt, tp_year, tp_time from full_data_type_table order by tp_year desc nulls first, id_dt limit 2",
      result)
  }

  test("char/varchar type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_varchar, tp_char, tp_nvarchar from full_data_type_table order by tp_varchar desc nulls first, id_dt limit 10",
      qTiDB =
        "select id_dt, tp_varchar, tp_char, tp_nvarchar from full_data_type_table order by tp_varchar is null desc, tp_varchar desc, id_dt limit 10")
  }

  test("blob/text type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_blob, tp_longtext, tp_mediumtext, tp_text, tp_tinytext from full_data_type_table order by tp_text desc nulls first limit 10",
      qTiDB =
        "select id_dt, tp_blob, tp_longtext, tp_mediumtext, tp_text, tp_tinytext from full_data_type_table order by tp_text is null desc, tp_text desc limit 10")
  }

  test("bit/binary type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_bit, tp_binary from full_data_type_table order by tp_binary desc nulls first, id_dt limit 10",
      qTiDB =
        "select id_dt, tp_bit, tp_binary from full_data_type_table order by tp_binary is null desc, tp_binary desc, id_dt limit 10")
  }

  test("enum type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_enum from full_data_type_table order by tp_enum desc nulls last, id_dt limit 10",
      qTiDB =
        "select id_dt, tp_enum from full_data_type_table order by tp_enum desc, id_dt limit 10")
  }

  test("set type test") {
    compSparkWithTiDB(
      qSpark =
        "select id_dt, tp_set from full_data_type_table order by tp_set desc nulls last, id_dt limit 10",
      qTiDB =
        "select id_dt, tp_set from full_data_type_table order by tp_set desc, id_dt limit 10")
  }
}
