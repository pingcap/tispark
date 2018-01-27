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

class Union0Suite extends BaseTiSparkSuite with SharedSQLContext {

  test(
    "(select tp_decimal from full_data_type_table where tp_decimal < 0) union (select tp_decimal from full_data_type_table where tp_decimal > 0) order by tp_decimal"
  ) {
    runTest(
      "(select tp_decimal from full_data_type_table where tp_decimal < 0) union (select tp_decimal from full_data_type_table where tp_decimal > 0) order by tp_decimal",
      "(select tp_decimal from full_data_type_table_j where tp_decimal < 0) union (select tp_decimal from full_data_type_table_j where tp_decimal > 0) order by tp_decimal"
    )
  }

  test(
    "(select tp_smallint from full_data_type_table where tp_smallint < 0) union (select tp_smallint from full_data_type_table where tp_smallint > 0) order by tp_smallint"
  ) {
    runTest(
      "(select tp_smallint from full_data_type_table where tp_smallint < 0) union (select tp_smallint from full_data_type_table where tp_smallint > 0) order by tp_smallint",
      "(select tp_smallint from full_data_type_table_j where tp_smallint < 0) union (select tp_smallint from full_data_type_table_j where tp_smallint > 0) order by tp_smallint"
    )
  }

  test(
    "(select id_dt from full_data_type_table where id_dt < 0) union (select id_dt from full_data_type_table where id_dt > 0) order by id_dt"
  ) {
    runTest(
      "(select id_dt from full_data_type_table where id_dt < 0) union (select id_dt from full_data_type_table where id_dt > 0) order by id_dt",
      "(select id_dt from full_data_type_table_j where id_dt < 0) union (select id_dt from full_data_type_table_j where id_dt > 0) order by id_dt"
    )
  }

  test(
    "(select tp_int from full_data_type_table where tp_int < 0) union (select tp_int from full_data_type_table where tp_int > 0) order by tp_int"
  ) {
    runTest(
      "(select tp_int from full_data_type_table where tp_int < 0) union (select tp_int from full_data_type_table where tp_int > 0) order by tp_int",
      "(select tp_int from full_data_type_table_j where tp_int < 0) union (select tp_int from full_data_type_table_j where tp_int > 0) order by tp_int"
    )
  }

  test(
    "(select tp_bigint from full_data_type_table where tp_bigint < 0) union (select tp_bigint from full_data_type_table where tp_bigint > 0) order by tp_bigint"
  ) {
    runTest(
      "(select tp_bigint from full_data_type_table where tp_bigint < 0) union (select tp_bigint from full_data_type_table where tp_bigint > 0) order by tp_bigint",
      "(select tp_bigint from full_data_type_table_j where tp_bigint < 0) union (select tp_bigint from full_data_type_table_j where tp_bigint > 0) order by tp_bigint"
    )
  }

  test(
    "(select tp_mediumint from full_data_type_table where tp_mediumint < 0) union (select tp_mediumint from full_data_type_table where tp_mediumint > 0) order by tp_mediumint"
  ) {
    runTest(
      "(select tp_mediumint from full_data_type_table where tp_mediumint < 0) union (select tp_mediumint from full_data_type_table where tp_mediumint > 0) order by tp_mediumint",
      "(select tp_mediumint from full_data_type_table_j where tp_mediumint < 0) union (select tp_mediumint from full_data_type_table_j where tp_mediumint > 0) order by tp_mediumint"
    )
  }

  test(
    "(select tp_real from full_data_type_table where tp_real < 0) union (select tp_real from full_data_type_table where tp_real > 0) order by tp_real"
  ) {
    runTest(
      "(select tp_real from full_data_type_table where tp_real < 0) union (select tp_real from full_data_type_table where tp_real > 0) order by tp_real",
      "(select tp_real from full_data_type_table_j where tp_real < 0) union (select tp_real from full_data_type_table_j where tp_real > 0) order by tp_real"
    )
  }

  test(
    "(select tp_float from full_data_type_table where tp_float < 0) union (select tp_float from full_data_type_table where tp_float > 0) order by tp_float"
  ) {
    runTest(
      "(select tp_float from full_data_type_table where tp_float < 0) union (select tp_float from full_data_type_table where tp_float > 0) order by tp_float",
      "(select tp_float from full_data_type_table_j where tp_float < 0) union (select tp_float from full_data_type_table_j where tp_float > 0) order by tp_float"
    )
  }

  test(
    "(select tp_double from full_data_type_table where tp_double < 0) union (select tp_double from full_data_type_table where tp_double > 0) order by tp_double"
  ) {
    runTest(
      "(select tp_double from full_data_type_table where tp_double < 0) union (select tp_double from full_data_type_table where tp_double > 0) order by tp_double",
      "(select tp_double from full_data_type_table_j where tp_double < 0) union (select tp_double from full_data_type_table_j where tp_double > 0) order by tp_double"
    )
  }

  test(
    "(select tp_tinyint from full_data_type_table where tp_tinyint < 0) union (select tp_tinyint from full_data_type_table where tp_tinyint > 0) order by tp_tinyint"
  ) {
    runTest(
      "(select tp_tinyint from full_data_type_table where tp_tinyint < 0) union (select tp_tinyint from full_data_type_table where tp_tinyint > 0) order by tp_tinyint",
      "(select tp_tinyint from full_data_type_table_j where tp_tinyint < 0) union (select tp_tinyint from full_data_type_table_j where tp_tinyint > 0) order by tp_tinyint"
    )
  }

}
