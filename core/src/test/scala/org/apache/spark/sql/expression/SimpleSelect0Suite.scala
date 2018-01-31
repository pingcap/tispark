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

class SimpleSelect0Suite extends BaseTiSparkSuite with SharedSQLContext {

  test("select tp_text from full_data_type_table  order by tp_text  limit 10") {
    runTest(
      "select tp_text from full_data_type_table  order by tp_text  limit 10",
      "select tp_text from full_data_type_table_j  order by tp_text  limit 10"
    )
  }

  test("select tp_binary from full_data_type_table  order by tp_binary  ") {
    runTest(
      "select tp_binary from full_data_type_table  order by tp_binary  ",
      "select tp_binary from full_data_type_table_j  order by tp_binary  "
    )
  }

  test("select tp_float from full_data_type_table  order by tp_float  ") {
    runTest(
      "select tp_float from full_data_type_table  order by tp_float  ",
      "select tp_float from full_data_type_table_j  order by tp_float  "
    )
  }

  test("select tp_longtext from full_data_type_table  order by tp_longtext limit 10 ") {
    runTest(
      "select tp_longtext from full_data_type_table  order by tp_longtext  limit 10",
      "select tp_longtext from full_data_type_table_j  order by tp_longtext  limit 10"
    )
  }

  test("select tp_bigint from full_data_type_table  order by tp_bigint  ") {
    runTest(
      "select tp_bigint from full_data_type_table  order by tp_bigint  ",
      "select tp_bigint from full_data_type_table_j  order by tp_bigint  "
    )
  }

  test("select tp_timestamp from full_data_type_table  order by tp_timestamp  ") {
    runTest(
      "select tp_timestamp from full_data_type_table order by tp_timestamp  ",
      "select tp_timestamp from full_data_type_table_j order by tp_timestamp  "
    )
  }

  test("select tp_tinyint from full_data_type_table  order by tp_tinyint  ") {
    runTest(
      "select tp_tinyint from full_data_type_table  order by tp_tinyint  ",
      "select tp_tinyint from full_data_type_table_j  order by tp_tinyint  "
    )
  }

  test("select tp_char from full_data_type_table  order by tp_char  ") {
    runTest(
      "select tp_char from full_data_type_table  order by tp_char  ",
      "select tp_char from full_data_type_table_j  order by tp_char  "
    )
  }

  test("select tp_decimal from full_data_type_table  order by tp_decimal  ") {
    runTest(
      "select tp_decimal from full_data_type_table  order by tp_decimal  ",
      "select tp_decimal from full_data_type_table_j  order by tp_decimal  "
    )
  }

  test("select tp_date from full_data_type_table  order by tp_date  ") {
    runTest(
      "select tp_date from full_data_type_table order by tp_date ",
      "select tp_date from full_data_type_table_j order by tp_date "
    )
  }

  test("select tp_mediumint from full_data_type_table  order by tp_mediumint  ") {
    runTest(
      "select tp_mediumint from full_data_type_table  order by tp_mediumint  ",
      "select tp_mediumint from full_data_type_table_j  order by tp_mediumint  "
    )
  }

  test("select tp_datetime,id_dt from full_data_type_table  order by tp_datetime  ") {
    runTest(
      "select tp_datetime,id_dt from full_data_type_table  order by tp_datetime  ",
      "select tp_datetime,id_dt from full_data_type_table_j  order by tp_datetime  "
    )
  }

  test("select tp_mediumtext from full_data_type_table  order by tp_mediumtext limit 10 ") {
    runTest(
      "select tp_mediumtext from full_data_type_table  order by tp_mediumtext limit 10 ",
      "select tp_mediumtext from full_data_type_table_j  order by tp_mediumtext limit 10 "
    )
  }

  test("select tp_int from full_data_type_table  order by tp_int  ") {
    runTest(
      "select tp_int from full_data_type_table  order by tp_int  ",
      "select tp_int from full_data_type_table_j  order by tp_int  "
    )
  }

  test("select tp_double from full_data_type_table  order by tp_double  ") {
    runTest(
      "select tp_double from full_data_type_table  order by tp_double  ",
      "select tp_double from full_data_type_table_j  order by tp_double  "
    )
  }

  test("select tp_real from full_data_type_table  order by tp_real  ") {
    runTest(
      "select tp_real from full_data_type_table  order by tp_real  ",
      "select tp_real from full_data_type_table_j  order by tp_real  "
    )
  }

  test("select tp_smallint from full_data_type_table  order by tp_smallint  ") {
    runTest(
      "select tp_smallint from full_data_type_table  order by tp_smallint  ",
      "select tp_smallint from full_data_type_table_j  order by tp_smallint  "
    )
  }

  test("select tp_nvarchar from full_data_type_table  order by tp_nvarchar  ") {
    runTest(
      "select tp_nvarchar from full_data_type_table  order by tp_nvarchar  ",
      "select tp_nvarchar from full_data_type_table_j  order by tp_nvarchar  "
    )
  }

  test("select id_dt from full_data_type_table  order by id_dt  ") {
    runTest(
      "select id_dt from full_data_type_table  order by id_dt  ",
      "select id_dt from full_data_type_table_j  order by id_dt  "
    )
  }

  test("select tp_blob from full_data_type_table  order by tp_blob  ") {
    runTest(
      "select tp_blob from full_data_type_table  order by tp_blob  ",
      "select tp_blob from full_data_type_table_j  order by tp_blob  "
    )
  }

  test("select tp_varchar from full_data_type_table  order by tp_varchar  ") {
    runTest(
      "select tp_varchar from full_data_type_table  order by tp_varchar  ",
      "select tp_varchar from full_data_type_table_j  order by tp_varchar  "
    )
  }

  test("select tp_tinytext from full_data_type_table  order by tp_tinytext limit 10 ") {
    runTest(
      "select tp_tinytext from full_data_type_table  order by tp_tinytext limit 10",
      "select tp_tinytext from full_data_type_table_j  order by tp_tinytext limit 10"
    )
  }

}
