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

import org.apache.spark.sql.BaseTiSparkSuite
import org.apache.spark.sql.test.SharedSQLContext

class Distinct0Suite extends BaseTiSparkSuite with SharedSQLContext {

  test("select  distinct(tp_binary)  from full_data_type_table  order by tp_binary ") {
    runTest(
      "select  distinct(tp_binary)  from full_data_type_table  order by tp_binary ",
      "select  distinct(tp_binary)  from full_data_type_table_j  order by tp_binary "
    )
  }

  test("select  distinct(tp_nvarchar)  from full_data_type_table  order by tp_nvarchar ") {
    runTest(
      "select  distinct(tp_nvarchar)  from full_data_type_table  order by tp_nvarchar ",
      "select  distinct(tp_nvarchar)  from full_data_type_table_j  order by tp_nvarchar "
    )
  }

  test("select  distinct(tp_date)  from full_data_type_table  order by tp_date ") {
    runTest(
      "select  distinct(tp_date)  from full_data_type_table  order by tp_date ",
      "select  distinct(tp_date)  from full_data_type_table_j  order by tp_date "
    )
  }

  test("select  count(distinct(tp_date))  from full_data_type_table") {
    runTest(
      "select  count(distinct(tp_date))  from full_data_type_table",
      "select  count(distinct(tp_date))  from full_data_type_table_j"
    )
  }

  test("select  distinct(tp_tinyint)  from full_data_type_table  order by tp_tinyint ") {
    runTest(
      "select  distinct(tp_tinyint)  from full_data_type_table  order by tp_tinyint ",
      "select  distinct(tp_tinyint)  from full_data_type_table_j  order by tp_tinyint "
    )
  }

  test("select  distinct(tp_int)  from full_data_type_table  order by tp_int ") {
    runTest(
      "select  distinct(tp_int)  from full_data_type_table  order by tp_int ",
      "select  distinct(tp_int)  from full_data_type_table_j  order by tp_int "
    )
  }

  test("select  distinct(tp_mediumint)  from full_data_type_table  order by tp_mediumint ") {
    runTest(
      "select  distinct(tp_mediumint)  from full_data_type_table  order by tp_mediumint ",
      "select  distinct(tp_mediumint)  from full_data_type_table_j  order by tp_mediumint "
    )
  }

  test("select  distinct(tp_real)  from full_data_type_table  order by tp_real ") {
    runTest(
      "select  distinct(tp_real)  from full_data_type_table  order by tp_real ",
      "select  distinct(tp_real)  from full_data_type_table_j  order by tp_real "
    )
  }

  test("select  distinct(tp_blob)  from full_data_type_table  order by tp_blob ") {
    runTest(
      "select  distinct(tp_blob)  from full_data_type_table  order by tp_blob ",
      "select  distinct(tp_blob)  from full_data_type_table_j  order by tp_blob "
    )
  }

  test("select  distinct(tp_datetime)  from full_data_type_table  order by tp_datetime ") {
    runTest(
      "select  distinct(tp_datetime)  from full_data_type_table  order by tp_datetime ",
      "select  distinct(tp_datetime)  from full_data_type_table_j  order by tp_datetime "
    )
  }

  test("select  count(distinct(tp_datetime))  from full_data_type_table") {
    runTest(
      "select  count(distinct(tp_datetime))  from full_data_type_table",
      "select  count(distinct(tp_datetime))  from full_data_type_table_j"
    )
  }

  test("select  distinct(tp_varchar)  from full_data_type_table  order by tp_varchar ") {
    runTest(
      "select  distinct(tp_varchar)  from full_data_type_table  order by tp_varchar ",
      "select  distinct(tp_varchar)  from full_data_type_table_j  order by tp_varchar "
    )
  }

  test("select  distinct(id_dt)  from full_data_type_table  order by id_dt ") {
    runTest(
      "select  distinct(id_dt)  from full_data_type_table  order by id_dt ",
      "select  distinct(id_dt)  from full_data_type_table_j  order by id_dt "
    )
  }

  test("select  distinct(tp_float)  from full_data_type_table  order by tp_float ") {
    runTest(
      "select  distinct(tp_float)  from full_data_type_table  order by tp_float ",
      "select  distinct(tp_float)  from full_data_type_table_j  order by tp_float "
    )
  }

  test("select  distinct(tp_char)  from full_data_type_table  order by tp_char ") {
    runTest(
      "select  distinct(tp_char)  from full_data_type_table  order by tp_char ",
      "select  distinct(tp_char)  from full_data_type_table_j  order by tp_char "
    )
  }

  test("select  distinct(tp_decimal)  from full_data_type_table  order by tp_decimal ") {
    runTest(
      "select  distinct(tp_decimal)  from full_data_type_table  order by tp_decimal ",
      "select  distinct(tp_decimal)  from full_data_type_table_j  order by tp_decimal "
    )
  }

  test("select  distinct(tp_timestamp)  from full_data_type_table  order by tp_timestamp ") {
    runTest(
      "select  distinct(tp_timestamp)  from full_data_type_table  order by tp_timestamp ",
      "select  distinct(tp_timestamp)  from full_data_type_table_j  order by tp_timestamp "
    )
  }

  test("select  distinct(tp_bigint)  from full_data_type_table  order by tp_bigint ") {
    runTest(
      "select  distinct(tp_bigint)  from full_data_type_table  order by tp_bigint ",
      "select  distinct(tp_bigint)  from full_data_type_table_j  order by tp_bigint "
    )
  }

  test("select  distinct(tp_smallint)  from full_data_type_table  order by tp_smallint ") {
    runTest(
      "select  distinct(tp_smallint)  from full_data_type_table  order by tp_smallint ",
      "select  distinct(tp_smallint)  from full_data_type_table_j  order by tp_smallint "
    )
  }

  test("select  distinct(tp_double)  from full_data_type_table  order by tp_double ") {
    runTest(
      "select  distinct(tp_double)  from full_data_type_table  order by tp_double ",
      "select  distinct(tp_double)  from full_data_type_table_j  order by tp_double "
    )
  }

}
